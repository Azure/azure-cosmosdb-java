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
package com.microsoft.azure.cosmosdb.rx.internal;

import java.time.Duration;

import com.microsoft.azure.cosmosdb.DocumentClientException;
import com.microsoft.azure.cosmosdb.DocumentCollection;
import com.microsoft.azure.cosmosdb.FeedOptions;
import com.microsoft.azure.cosmosdb.internal.HttpConstants;
import com.microsoft.azure.cosmosdb.internal.OperationType;
import com.microsoft.azure.cosmosdb.internal.ResourceType;
import com.microsoft.azure.cosmosdb.internal.routing.CollectionRoutingMap;
import com.microsoft.azure.cosmosdb.rx.internal.caches.RxCollectionCache;
import com.microsoft.azure.cosmosdb.rx.internal.caches.IPartitionKeyRangeCache;
import reactor.core.publisher.Mono;

// TODO: this need testing
/**
 * While this class is public, but it is not part of our published public APIs.
 * This is meant to be internally used only by our sdk.
 */
public class PartitionKeyRangeGoneRetryPolicy implements IDocumentClientRetryPolicy {

    private final RxCollectionCache collectionCache;
    private final IDocumentClientRetryPolicy nextRetryPolicy;
    private final IPartitionKeyRangeCache partitionKeyRangeCache;
    private final String collectionLink;
    private final FeedOptions feedOptions;
    private volatile boolean retried;

    public PartitionKeyRangeGoneRetryPolicy(
            RxCollectionCache collectionCache,
            IPartitionKeyRangeCache partitionKeyRangeCache,
            String collectionLink,
            IDocumentClientRetryPolicy nextRetryPolicy,
            FeedOptions feedOptions) {
        this.collectionCache = collectionCache;
        this.partitionKeyRangeCache = partitionKeyRangeCache;
        this.collectionLink = collectionLink;
        this.nextRetryPolicy = nextRetryPolicy;
        this.feedOptions = feedOptions;
    }

    /// <summary> 
    /// Should the caller retry the operation.
    /// </summary>
    /// <param name="exception">Exception that occured when the operation was tried</param>
    /// <param name="cancellationToken"></param>
    /// <returns>True indicates caller should retry, False otherwise</returns>
    public Mono<ShouldRetryResult> shouldRetry(Exception exception) {
        DocumentClientException clientException = Utils.as(exception, DocumentClientException.class);
        if (clientException != null && 
                Exceptions.isStatusCode(clientException, HttpConstants.StatusCodes.GONE) &&
                Exceptions.isSubStatusCode(clientException, HttpConstants.SubStatusCodes.PARTITION_KEY_RANGE_GONE)) {

            if (this.retried){
                return Mono.just(ShouldRetryResult.error(clientException));
            }

            RxDocumentServiceRequest request = RxDocumentServiceRequest.create(
                    OperationType.Read,
                    ResourceType.DocumentCollection,
                    this.collectionLink,
                    null
                    // AuthorizationTokenType.PrimaryMasterKey)
                    );
            if (this.feedOptions != null) {
                request.properties = this.feedOptions.getProperties();
            }
            Mono<DocumentCollection> collectionObs = this.collectionCache.resolveCollectionAsync(request);

            return collectionObs.flatMap(collection -> {

                Mono<CollectionRoutingMap> routingMapObs = this.partitionKeyRangeCache.tryLookupAsync(collection.getResourceId(), null, request.properties);

                Mono<CollectionRoutingMap> refreshedRoutingMapObs = routingMapObs.flatMap(routingMap -> {
                    // Force refresh.
                    return this.partitionKeyRangeCache.tryLookupAsync(
                            collection.getResourceId(),
                            routingMap,
                            request.properties);
                }).switchIfEmpty(Mono.defer(Mono::empty));

                //  TODO: Check if this behavior can be replaced by doOnSubscribe
                return refreshedRoutingMapObs.flatMap(rm -> {
                    this.retried = true;
                    return Mono.just(ShouldRetryResult.retryAfter(Duration.ZERO));
                }).switchIfEmpty(Mono.defer(() -> {
                    this.retried = true;
                    return Mono.just(ShouldRetryResult.retryAfter(Duration.ZERO));
                }));

            });

        } else {
            return this.nextRetryPolicy.shouldRetry(exception);
        }
    }

    @Override
    public void onBeforeSendRequest(RxDocumentServiceRequest request) {
        this.nextRetryPolicy.onBeforeSendRequest(request);        
    }

}
