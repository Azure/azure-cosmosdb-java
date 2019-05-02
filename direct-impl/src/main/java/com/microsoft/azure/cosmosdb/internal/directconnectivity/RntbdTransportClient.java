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
import com.microsoft.azure.cosmosdb.internal.directconnectivity.rntbd.RntbdRequestArgs;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.rntbd.RntbdRequestTimer;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.rntbd.RntbdServiceEndpoint;
import com.microsoft.azure.cosmosdb.rx.internal.Configs;
import com.microsoft.azure.cosmosdb.rx.internal.RxDocumentServiceRequest;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.ssl.SslContext;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Single;
import rx.SingleEmitter;

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
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
    private final EndpointFactory endpointFactory;
    private final Metrics metrics;
    private final String name;

    // endregion

    // region Constructors

    RntbdTransportClient(final EndpointFactory endpointFactory) {
        this.name = RntbdTransportClient.className + '-' + RntbdTransportClient.instanceCount.incrementAndGet();
        this.endpointFactory = endpointFactory;
        this.metrics = new Metrics();
    }

    RntbdTransportClient(final Options options, final SslContext sslContext) {
        this(new EndpointFactory(options, sslContext));
    }

    RntbdTransportClient(final Configs configs, final int requestTimeoutInSeconds, final UserAgentContainer userAgent) {
        this(new Options.Builder()
                .requestTimeout(Duration.ofSeconds(requestTimeoutInSeconds))
                .userAgent(userAgent)
                .build(),
            configs.getSslContext()
        );
    }

    // endregion

    // region Methods

    @Override
    public void close() {

        if (this.closed.compareAndSet(false, true)) {

            this.endpointFactory.close().addListener(future -> {
                if (future.isSuccess()) {
                    logger.debug("{} closed endpoints", this);
                    return;
                }
                logger.error("{} failed to close endpoints due to {}", this, future.cause());
            });

        } else {
            logger.debug("{} already closed", this);
        }
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

        final Endpoint endpoint = this.endpointFactory.getEndpoint(physicalAddress);
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
        final long endpointCount = this.endpointFactory.endpoints.mappingCount();
        return '[' + this.name + "(endpointCount: " + endpointCount + ", " + this.metrics + ")]";
    }

    private void throwIfClosed() {
        if (this.closed.get()) {
            throw new IllegalStateException(String.format("%s is closed", this));
        }
    }

    // endregion

    // region Types

    public interface Endpoint {

        void close();

        CompletableFuture<StoreResponse> request(RntbdRequestArgs requestArgs);
    }

    public static final class Config {

        private final Options options;
        private final SslContext sslContext;
        private final LogLevel wireLogLevel;

        Config(final SslContext sslContext, final LogLevel wireLogLevel, final Options options) {

            checkNotNull(sslContext, "sslContext");
            checkNotNull(options, "options");

            this.sslContext = sslContext;
            this.wireLogLevel = wireLogLevel;
            this.options = options;
        }

        public int getConnectionTimeout() {
            final long value = this.options.getOpenTimeout().toMillis();
            assert value <= Integer.MAX_VALUE;
            return (int)value;
        }

        public int getMaxChannelsPerEndpoint() {
            return this.options.getMaxChannelsPerEndpoint();
        }

        public int getMaxRequestsPerChannel() {
            return this.options.getMaxRequestsPerChannel();
        }

        public Options getOptions() {
            return this.options;
        }

        public long getReceiveHangDetectionTime() {
            return this.options.getReceiveHangDetectionTime().toNanos();

        }

        public long getSendHangDetectionTime() {
            return this.options.getSendHangDetectionTime().toNanos();
        }

        public SslContext getSslContext() {
            return this.sslContext;
        }

        public UserAgentContainer getUserAgent() {
            return this.options.getUserAgent();
        }

        public LogLevel getWireLogLevel() {
            return this.wireLogLevel;
        }
    }

    static class EndpointFactory {

        private final ConcurrentHashMap<String, Endpoint> endpoints = new ConcurrentHashMap<>();
        private final NioEventLoopGroup eventLoopGroup;
        private final Config pipelineConfig;
        private final RntbdRequestTimer requestTimer;

        EndpointFactory(final Options options, final SslContext sslContext) {

            checkNotNull(options, "options");
            checkNotNull(sslContext, "sslContext");

            final DefaultThreadFactory threadFactory = new DefaultThreadFactory("CosmosEventLoop", true);
            final int threadCount = Runtime.getRuntime().availableProcessors();
            final LogLevel wireLogLevel;

            if (RntbdTransportClient.logger.isTraceEnabled()) {
                wireLogLevel = LogLevel.TRACE;
            } else if (RntbdTransportClient.logger.isDebugEnabled()) {
                wireLogLevel = LogLevel.DEBUG;
            } else {
                wireLogLevel = null;
            }

            this.requestTimer = new RntbdRequestTimer(options.getRequestTimeout());
            this.eventLoopGroup = new NioEventLoopGroup(threadCount, threadFactory);
            this.pipelineConfig = new Config(sslContext, wireLogLevel, options);
        }

        Config getPipelineConfig() {
            return this.pipelineConfig;
        }

        RntbdRequestTimer getRequestTimer() {
            return this.requestTimer;
        }

        Future<?> close() {
        
            this.requestTimer.close();
            
            for (final Endpoint endpoint : this.endpoints.values()) {
                endpoint.close();
            }
            
            return this.eventLoopGroup.shutdownGracefully();
        }

        Endpoint createEndpoint(final URI physicalAddress) {
            return new RntbdServiceEndpoint(this.pipelineConfig, this.eventLoopGroup, this.requestTimer, physicalAddress);
        }

        void deleteEndpoint(final URI physicalAddress) {

            // TODO: DANOBLE: Utilize this method of tearing down unhealthy endpoints
            //  Links:
            //  https://msdata.visualstudio.com/CosmosDB/_workitems/edit/331552
            //  https://msdata.visualstudio.com/CosmosDB/_workitems/edit/331593

            final String authority = physicalAddress.getAuthority();
            final Endpoint endpoint = this.endpoints.remove(authority);

            checkNotNull(endpoint, "physicalAddress: %s", physicalAddress);
            endpoint.close();
        }

        Endpoint getEndpoint(final URI physicalAddress) {
            return this.endpoints.computeIfAbsent(
                physicalAddress.getAuthority(), authority -> this.createEndpoint(physicalAddress)
            );
        }
    }

    public static final class Metrics {

        private final Stopwatch lifetime = Stopwatch.createStarted();
        private final AtomicLong requestCount = new AtomicLong();
        private final AtomicLong responseCount = new AtomicLong();
        private final AtomicLong requestFailureCount = new AtomicLong();

        public final Stopwatch getLifetime() {
            return this.lifetime;
        }

        public final long getRequestCount() {
            return this.requestCount.get();
        }

        public final double getRequestsPerSecond() {
            return this.responseCount.get() / (1E-9 * this.lifetime.elapsed().toNanos());
        }

        public final long getResponseCount() {
            return this.responseCount.get();
        }

        public final void incrementRequestCount() {
            this.requestCount.incrementAndGet();
        }

        public final void incrementRequestFailureCount() {
            this.requestFailureCount.incrementAndGet();
        }

        public final void incrementResponseCount() {
            this.responseCount.incrementAndGet();
        }

        @Override
        public String toString() {
            return "lifetime: " + this.lifetime + ", requestCount: " + this.requestCount + ", responseCount: "
                + this.responseCount + ", requestFailureCount: " + this.requestFailureCount;
        }
    }

    public static final class Options {

        // region Fields

        private final String certificateHostNameOverride;
        private final int maxChannelsPerEndpoint;
        private final int maxRequestsPerChannel;
        private final Duration openTimeout;
        private final int partitionCount;
        private final Duration receiveHangDetectionTime;
        private final Duration requestTimeout;
        private final Duration sendHangDetectionTime;
        private final UserAgentContainer userAgent;

        // endregion

        // region Constructors

        public Options(final Duration requestTimeout) {
            this(new Builder().requestTimeout(requestTimeout));
        }

        public Options(final int requestTimeoutInSeconds) {
            this(Duration.ofSeconds((long)requestTimeoutInSeconds));
        }

        private Options(Builder builder) {

            checkNotNull(builder.requestTimeout, "requestTimeout: null");

            this.certificateHostNameOverride = builder.certificateHostNameOverride;
            this.maxChannelsPerEndpoint = builder.maxChannelsPerEndpoint;
            this.maxRequestsPerChannel = builder.maxRequestsPerChannel;
            this.openTimeout = builder.openTimeout == null ? builder.requestTimeout : builder.openTimeout;
            this.partitionCount = builder.partitionCount;
            this.requestTimeout = builder.requestTimeout;
            this.receiveHangDetectionTime = builder.receiveHangDetectionTime;
            this.sendHangDetectionTime = builder.sendHangDetectionTime;
            this.userAgent = builder.userAgent;
        }

        // endregion

        // region Property accessors

        public String getCertificateHostNameOverride() {
            return this.certificateHostNameOverride;
        }

        public int getMaxChannelsPerEndpoint() {
            return this.maxChannelsPerEndpoint;
        }

        public int getMaxRequestsPerChannel() {
            return this.maxRequestsPerChannel;
        }

        public Duration getOpenTimeout() {
            return this.openTimeout;
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

        // region Types

        public static class Builder {

            private static final UserAgentContainer DEFAULT_USER_AGENT_CONTAINER = new UserAgentContainer();
            private static final Duration SIXTY_FIVE_SECONDS = Duration.ofSeconds(65L);
            private static final Duration TEN_SECONDS = Duration.ofSeconds(10L);

            private String certificateHostNameOverride = null;
            private int maxChannelsPerEndpoint = 10;
            private int maxRequestsPerChannel = 30;
            private Duration openTimeout = null;
            private int partitionCount = 1;
            private Duration receiveHangDetectionTime = SIXTY_FIVE_SECONDS;
            private Duration requestTimeout = null;
            private Duration sendHangDetectionTime = TEN_SECONDS;
            private UserAgentContainer userAgent = DEFAULT_USER_AGENT_CONTAINER;

            Options build() {
                return new Options(this);
            }

            Builder certificateHostNameOverride(final String value) {
                this.certificateHostNameOverride = value;
                return this;
            }

            Builder maxChannelsPerEndpoint(final int value) {
                checkArgument(value > 0, "value: %s", value);
                this.maxChannelsPerEndpoint = value;
                return this;
            }

            public Builder maxRequestsPerChannel(final int value) {
                checkArgument(value > 0, "value: %s", value);
                this.maxRequestsPerChannel = value;
                return this;
            }

            public Builder openTimeout(final Duration value) {
                checkArgument(value == null || value.compareTo(Duration.ZERO) > 0, "value: %s", value);
                this.openTimeout = value;
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
        }

        // endregion
    }

    // endregion
}
