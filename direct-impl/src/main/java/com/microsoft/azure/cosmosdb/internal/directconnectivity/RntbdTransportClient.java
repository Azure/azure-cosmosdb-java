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
 *
 */

package com.microsoft.azure.cosmosdb.internal.directconnectivity;

import com.google.common.base.Stopwatch;
import com.microsoft.azure.cosmosdb.DocumentClientException;
import com.microsoft.azure.cosmosdb.internal.UserAgentContainer;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.rntbd.RntbdEndpoint;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.rntbd.RntbdObjectMapper;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.rntbd.RntbdRequestArgs;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.rntbd.RntbdServiceEndpoint;
import com.microsoft.azure.cosmosdb.rx.internal.Configs;
import com.microsoft.azure.cosmosdb.rx.internal.RxDocumentServiceRequest;
import io.netty.handler.ssl.SslContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Single;
import rx.SingleEmitter;

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public final class RntbdTransportClient extends TransportClient implements AutoCloseable {

    // region Fields

    private static final String className = RntbdTransportClient.class.getCanonicalName();
    private static final AtomicLong instanceCount = new AtomicLong();
    private static final Logger logger = LoggerFactory.getLogger(className);

    private final AtomicBoolean closed = new AtomicBoolean();
    private final RntbdEndpoint.Provider endpointProvider;
    private final Metrics metrics;
    private final String name;

    // endregion

    // region Constructors

    RntbdTransportClient(final RntbdEndpoint.Provider endpointProvider) {
        this.name = RntbdTransportClient.className + '-' + RntbdTransportClient.instanceCount.incrementAndGet();
        this.endpointProvider = endpointProvider;
        this.metrics = new Metrics();
    }

    RntbdTransportClient(final Options options, final SslContext sslContext) {
        this(new RntbdServiceEndpoint.Provider(options, sslContext));
    }

    RntbdTransportClient(final Configs configs, final int requestTimeoutInSeconds, final UserAgentContainer userAgent) {
        this(new Options.Builder(requestTimeoutInSeconds).userAgent(userAgent).build(), configs.getSslContext());
    }

    // endregion

    // region Methods

    @Override
    public void close() throws RuntimeException {
        if (this.closed.compareAndSet(false, true)) {
            this.endpointProvider.close();
            return;
        }
        logger.debug("{} already closed", this);
    }

    @Override
    public Single<StoreResponse> invokeStoreAsync(
        final URI physicalAddress, final ResourceOperation unused, final RxDocumentServiceRequest request
    ) {
        checkNotNull(physicalAddress, "physicalAddress");
        checkNotNull(request, "request");
        this.throwIfClosed();

        final RntbdRequestArgs requestArgs = new RntbdRequestArgs(request, physicalAddress);

        if (logger.isDebugEnabled()) {
            requestArgs.traceOperation(logger, null, "invokeStoreAsync");
            logger.debug("\n  {}\n  {}\n  INVOKE_STORE_ASYNC", this, requestArgs);
        }

        final RntbdEndpoint endpoint = this.endpointProvider.get(physicalAddress);
        this.metrics.incrementRequestCount();

        final CompletableFuture<StoreResponse> future = endpoint.request(requestArgs);

        return Single.fromEmitter((SingleEmitter<StoreResponse> emitter) -> {

            future.whenComplete((response, error) -> {

                requestArgs.traceOperation(logger, null, "emitSingle", response, error);
                this.metrics.incrementResponseCount();

                if (error == null) {
                    assert response != null;
                    emitter.onSuccess(response);
                } else {
                    assert error instanceof DocumentClientException && response == null;
                    this.metrics.incrementRequestFailureCount();
                    emitter.onError(error);
                }

                requestArgs.traceOperation(logger, null, "emitSingleComplete");
            });
        });
    }

    @Override
    public String toString() {
        return '[' + this.name + "(endpointCount: " + this.endpointProvider.count() + ", " + this.metrics + ")]";
    }

    private void throwIfClosed() {
        if (this.closed.get()) {
            throw new IllegalStateException(String.format("%s is closed", this));
        }
    }

    // endregion

    // region Types

    public static final class Metrics {

        // region Fields

        private final AtomicLong errorResponseCount = new AtomicLong();
        private final Stopwatch lifetime = Stopwatch.createStarted();
        private final AtomicLong requestCount = new AtomicLong();
        private final AtomicLong responseCount = new AtomicLong();

        // endregion

        // region Accessors

        public double getFailureRate() {
            return this.errorResponseCount.get() / this.requestCount.get();
        }

        public Stopwatch getLifetime() {
            return this.lifetime;
        }

        public long getPendingRequestCount() {
            return this.requestCount.get() - this.responseCount.get();
        }

        public long getRequestCount() {
            return this.requestCount.get();
        }

        public long getResponseCount() {
            return this.responseCount.get();
        }

        public double getSuccessRate() {
            return (this.responseCount.get() - this.errorResponseCount.get()) / this.requestCount.get();
        }

        public double getThroughput() {
            return this.responseCount.get() / (1E-9 * this.lifetime.elapsed().toNanos());
        }

        // endregion

        // region Methods

        public final void incrementRequestCount() {
            this.requestCount.incrementAndGet();
        }

        public final void incrementRequestFailureCount() {
            this.errorResponseCount.incrementAndGet();
        }

        public final void incrementResponseCount() {
            this.responseCount.incrementAndGet();
        }

        @Override
        public String toString() {
            return RntbdObjectMapper.toJson(this);
        }

        // endregion
    }

    public static final class Options {

        // region Fields

        private final String certificateHostNameOverride;
        private final int maxChannelsPerEndpoint;
        private final int maxRequestsPerChannel;
        private final Duration connectionTimeout;
        private final int partitionCount;
        private final Duration receiveHangDetectionTime;
        private final Duration requestTimeout;
        private final Duration sendHangDetectionTime;
        private final UserAgentContainer userAgent;

        // endregion

        // region Constructors

        private Options(Builder builder) {

            this.certificateHostNameOverride = builder.certificateHostNameOverride;
            this.maxChannelsPerEndpoint = builder.maxChannelsPerEndpoint;
            this.maxRequestsPerChannel = builder.maxRequestsPerChannel;
            this.connectionTimeout = builder.connectionTimeout == null ? builder.requestTimeout : builder.connectionTimeout;
            this.partitionCount = builder.partitionCount;
            this.requestTimeout = builder.requestTimeout;
            this.receiveHangDetectionTime = builder.receiveHangDetectionTime;
            this.sendHangDetectionTime = builder.sendHangDetectionTime;
            this.userAgent = builder.userAgent;
        }

        // endregion

        // region Accessors

        public String getCertificateHostNameOverride() {
            return this.certificateHostNameOverride;
        }

        public int getMaxChannelsPerEndpoint() {
            return this.maxChannelsPerEndpoint;
        }

        public int getMaxRequestsPerChannel() {
            return this.maxRequestsPerChannel;
        }

        public Duration getConnectionTimeout() {
            return this.connectionTimeout;
        }

        public int getPartitionCount() {
            return this.partitionCount;
        }

        public Duration getReceiveHangDetectionTime() {
            return this.receiveHangDetectionTime;
        }

        public Duration getRequestTimeout() {
            return this.requestTimeout;
        }

        public Duration getSendHangDetectionTime() {
            return this.sendHangDetectionTime;
        }

        public UserAgentContainer getUserAgent() {
            return this.userAgent;
        }

        // endregion

        // region Methods

        @Override
        public String toString() {
            return RntbdObjectMapper.toJson(this);
        }

        // endregion

        // region Types

        public static class Builder {

            // region Fields

            private static final UserAgentContainer DEFAULT_USER_AGENT_CONTAINER = new UserAgentContainer();
            private static final Duration SIXTY_FIVE_SECONDS = Duration.ofSeconds(65L);
            private static final Duration TEN_SECONDS = Duration.ofSeconds(10L);

            // Required parameters

            private String certificateHostNameOverride = null;

            // Optional parameters

            private int maxChannelsPerEndpoint = 10;
            private int maxRequestsPerChannel = 30;
            private Duration connectionTimeout = null;
            private int partitionCount = 1;
            private Duration receiveHangDetectionTime = SIXTY_FIVE_SECONDS;
            private Duration requestTimeout;
            private Duration sendHangDetectionTime = TEN_SECONDS;
            private UserAgentContainer userAgent = DEFAULT_USER_AGENT_CONTAINER;

            // endregion

            // region Constructors

            public Builder(Duration requestTimeout) {
                this.requestTimeout(requestTimeout);
            }

            public Builder(int requestTimeoutInSeconds) {
                this(Duration.ofSeconds(requestTimeoutInSeconds));
            }

            // endregion

            // region Methods

            public Options build() {
                return new Options(this);
            }

            public Builder certificateHostNameOverride(final String value) {
                this.certificateHostNameOverride = value;
                return this;
            }

            public Builder connectionTimeout(final Duration value) {
                checkArgument(value == null || value.compareTo(Duration.ZERO) > 0, "value: %s", value);
                this.connectionTimeout = value;
                return this;
            }

            public Builder maxRequestsPerChannel(final int value) {
                checkArgument(value > 0, "value: %s", value);
                this.maxRequestsPerChannel = value;
                return this;
            }

            public Builder maxChannelsPerEndpoint(final int value) {
                checkArgument(value > 0, "value: %s", value);
                this.maxChannelsPerEndpoint = value;
                return this;
            }

            public Builder partitionCount(final int value) {
                checkArgument(value > 0, "value: %s", value);
                this.partitionCount = value;
                return this;
            }

            public Builder receiveHangDetectionTime(final Duration value) {

                checkNotNull(value, "value: null");
                checkArgument(value.compareTo(Duration.ZERO) > 0, "value: %s", value);

                this.receiveHangDetectionTime = value;
                return this;
            }

            public Builder requestTimeout(final Duration value) {

                checkNotNull(value, "value: null");
                checkArgument(value.compareTo(Duration.ZERO) > 0, "value: %s", value);

                this.requestTimeout = value;
                return this;
            }

            public Builder sendHangDetectionTime(final Duration value) {

                checkNotNull(value, "value: null");
                checkArgument(value.compareTo(Duration.ZERO) > 0, "value: %s", value);

                this.sendHangDetectionTime = value;
                return this;
            }

            public Builder userAgent(final UserAgentContainer value) {
                checkNotNull(value, "value: null");
                this.userAgent = value;
                return this;
            }

            // endregion
        }

        // endregion
    }

    // endregion
}
