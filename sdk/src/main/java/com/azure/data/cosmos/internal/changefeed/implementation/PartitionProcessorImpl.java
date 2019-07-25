/*
 * The MIT License (MIT)
 * Copyright (c) 2018 Microsoft Corporation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.azure.data.cosmos.internal.changefeed.implementation;

import com.azure.data.cosmos.ChangeFeedOptions;
import com.azure.data.cosmos.CosmosClientException;
import com.azure.data.cosmos.CosmosItemProperties;
import com.azure.data.cosmos.FeedResponse;
import com.azure.data.cosmos.internal.changefeed.CancellationToken;
import com.azure.data.cosmos.internal.changefeed.ChangeFeedContextClient;
import com.azure.data.cosmos.internal.changefeed.ChangeFeedObserver;
import com.azure.data.cosmos.internal.changefeed.ChangeFeedObserverContext;
import com.azure.data.cosmos.internal.changefeed.PartitionCheckpointer;
import com.azure.data.cosmos.internal.changefeed.PartitionProcessor;
import com.azure.data.cosmos.internal.changefeed.ProcessorSettings;
import com.azure.data.cosmos.internal.changefeed.exceptions.PartitionNotFoundException;
import com.azure.data.cosmos.internal.changefeed.exceptions.PartitionSplitException;
import com.azure.data.cosmos.internal.changefeed.exceptions.TaskCancelledException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

import static com.azure.data.cosmos.CommonsBridgeInternal.partitionKeyRangeIdInternal;

/**
 * Implementation for {@link PartitionProcessor}.
 */
class PartitionProcessorImpl implements PartitionProcessor {
    private final Logger logger = LoggerFactory.getLogger(PartitionProcessorImpl.class);

    private static final int DefaultMaxItemCount = 100;
    // private final Observable<FeedResponse<Document>> query;
    private final ProcessorSettings settings;
    private final PartitionCheckpointer checkpointer;
    private final ChangeFeedObserver observer;
    private final ChangeFeedOptions options;
    private final ChangeFeedContextClient documentClient;
    private RuntimeException resultException;

    private String lastContinuation;

    public PartitionProcessorImpl(ChangeFeedObserver observer, ChangeFeedContextClient documentClient, ProcessorSettings settings, PartitionCheckpointer checkpointer) {
        this.observer = observer;
        this.documentClient = documentClient;
        this.settings = settings;
        this.checkpointer = checkpointer;

        this.options = new ChangeFeedOptions();
        this.options.maxItemCount(settings.getMaxItemCount());
        partitionKeyRangeIdInternal(this.options, settings.getPartitionKeyRangeId());
        // this.options.sessionToken(properties.sessionToken());
        this.options.startFromBeginning(settings.isStartFromBeginning());
        this.options.requestContinuation(settings.getStartContinuation());
        this.options.startDateTime(settings.getStartTime());
    }

    @Override
    public Mono<Void> run(CancellationToken cancellationToken) {
        PartitionProcessorImpl self = this;
        this.lastContinuation = this.settings.getStartContinuation();

        self.options.requestContinuation(self.lastContinuation);

        return self.documentClient.createDocumentChangeFeedQuery(self.settings.getCollectionSelfLink(), self.options)
            .flatMap(documentFeedResponse -> {
                if (cancellationToken.isCancellationRequested()) throw Exceptions.propagate(new TaskCancelledException());

                self.lastContinuation = documentFeedResponse.continuationToken();
                if (documentFeedResponse.results() != null && documentFeedResponse.results().size() > 0) {
                    self.dispatchChanges(documentFeedResponse);
                }
                self.options.requestContinuation(self.lastContinuation);

                if (cancellationToken.isCancellationRequested()) throw Exceptions.propagate(new TaskCancelledException());

                return Flux.just(documentFeedResponse);
            })
            .doOnComplete(() -> {
                if (this.options.maxItemCount().compareTo(this.settings.getMaxItemCount()) != 0) {
                    this.options.maxItemCount(this.settings.getMaxItemCount());   // Reset after successful execution.
                }
            })
            .onErrorResume(throwable -> {
                if (throwable instanceof CosmosClientException) {

                    CosmosClientException clientException = (CosmosClientException) throwable;
                    self.logger.warn("Exception: partition {}", self.options.partitionKey().getInternalPartitionKey(), clientException);
                    StatusCodeErrorType docDbError = ExceptionClassifier.classifyClientException(clientException);

                    switch (docDbError) {
                        case PARTITION_NOT_FOUND: {
                            self.resultException = new PartitionNotFoundException("Partition not found.", self.lastContinuation);
                        }
                        case PARTITION_SPLIT: {
                            self.resultException = new PartitionSplitException("Partition split.", self.lastContinuation);
                        }
                        case UNDEFINED: {
                            self.resultException = new RuntimeException(clientException);
                        }
                        case MAX_ITEM_COUNT_TOO_LARGE: {
                            if (this.options.maxItemCount() == null) {
                                this.options.maxItemCount(DefaultMaxItemCount);
                            } else if (this.options.maxItemCount() <= 1) {
                                self.logger.error("Cannot reduce maxItemCount further as it's already at {}", self.options.maxItemCount(), clientException);
                                self.resultException = new RuntimeException(clientException);
                            }

                            this.options.maxItemCount(this.options.maxItemCount() / 2);
                            self.logger.warn("Reducing maxItemCount, new value: {}", self.options.maxItemCount());
                            return Flux.empty();
                        }
                        default: {
                            self.logger.error("Unrecognized DocDbError enum value {}", docDbError, clientException);
                            self.resultException = new RuntimeException(clientException);
                        }
                    }
                } else if (throwable instanceof TaskCancelledException) {
                    // this.logger.WarnException("exception: partition '{0}'", canceledException, this.properties.PartitionKeyRangeId);
                    self.resultException = (TaskCancelledException) throwable;
                }
                return Flux.error(throwable);
            })
            .repeat(() -> {
                if (cancellationToken.isCancellationRequested()) {
                    self.resultException = new TaskCancelledException();
                    return false;
                }

                Duration delay = self.settings.getFeedPollDelay();
                long remainingWork = delay.toMillis();

                try {
                    while (!cancellationToken.isCancellationRequested() && remainingWork > 0) {
                        Thread.sleep(100);
                        remainingWork -= 100;
                    }
                } catch (InterruptedException iex) {
                    // exception caught
                    return false;
                }

                if (cancellationToken.isCancellationRequested()) {
                    self.resultException = new TaskCancelledException();
                    return false;
                }

                return true;
            })
            .then();
    }

    @Override
    public RuntimeException getResultException() {
        return this.resultException;
    }

    private void dispatchChanges(FeedResponse<CosmosItemProperties> response) {
        ChangeFeedObserverContext context = new ChangeFeedObserverContextImpl(this.settings.getPartitionKeyRangeId(), response, this.checkpointer);

        this.observer.processChanges(context, response.results());
    }
}
