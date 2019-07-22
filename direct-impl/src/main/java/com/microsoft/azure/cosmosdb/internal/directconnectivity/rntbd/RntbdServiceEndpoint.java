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

package com.microsoft.azure.cosmosdb.internal.directconnectivity.rntbd;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.microsoft.azure.cosmosdb.BridgeInternal;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.GoneException;
import io.micrometer.core.instrument.Tag;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.ssl.SslContext;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.microsoft.azure.cosmosdb.internal.HttpConstants.HttpHeaders;
import static com.microsoft.azure.cosmosdb.internal.directconnectivity.RntbdTransportClient.Options;

@JsonSerialize(using = RntbdServiceEndpoint.JsonSerializer.class)
public final class RntbdServiceEndpoint implements RntbdEndpoint {

    private static final long QUIET_PERIOD = 2L * 1_000_000_000L;

    private static final AtomicLong instanceCount = new AtomicLong();
    private static final Logger logger = LoggerFactory.getLogger(RntbdServiceEndpoint.class);
    private static final AdaptiveRecvByteBufAllocator receiveBufferAllocator = new AdaptiveRecvByteBufAllocator();

    private final RntbdClientChannelPool channelPool;
    private final AtomicBoolean closed;
    private final long id;
    private final SocketAddress remoteAddress;
    private final RntbdRequestTimer requestTimer;
    private final Tag tag;

    // region Constructors

    private RntbdServiceEndpoint(
        final Config config, final NioEventLoopGroup group, final RntbdRequestTimer timer, final URI physicalAddress
    ) {

        final Bootstrap bootstrap = new Bootstrap()
            .channel(NioSocketChannel.class)
            .group(group)
            .option(ChannelOption.ALLOCATOR, config.allocator())
            .option(ChannelOption.AUTO_READ, true)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.connectionTimeout())
            .option(ChannelOption.RCVBUF_ALLOCATOR, receiveBufferAllocator)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .remoteAddress(physicalAddress.getHost(), physicalAddress.getPort());

        this.channelPool = new RntbdClientChannelPool(bootstrap, config);
        this.closed = new AtomicBoolean();
        this.remoteAddress = bootstrap.config().remoteAddress();
        this.requestTimer = timer;

        this.tag = Tag.of(RntbdServiceEndpoint.class.getSimpleName(), this.remoteAddress.toString());
        this.id = instanceCount.incrementAndGet();
    }

    // endregion

    // region Accessors

    @Override
    public int acquiredChannels() {
        return this.channelPool.channelsAcquired();
    }

    @Override
    public int availableChannels() {
        return this.channelPool.channelsAvailable();
    }

    @Override
    public long id() {
        return this.id;
    }

    @Override
    public boolean isClosed() {
        return this.closed.get();
    }

    @Override
    public SocketAddress remoteAddress() {
        return this.remoteAddress;
    }

    @Override
    public int requestQueueLength() {
        return this.channelPool.requestQueueLength();
    }

    @Override
    public Tag tag() {
        return this.tag;
    }

    @Override
    public long usedDirectMemory() {
        return this.channelPool.usedDirectMemory();
    }

    @Override
    public long usedHeapMemory() {
        return this.channelPool.usedHeapMemory();
    }

    // endregion

    // region Methods

    @Override
    public void close() {
        if (this.closed.compareAndSet(false, true)) {
            this.channelPool.close();
        }
    }

    public RntbdRequestRecord request(final RntbdRequestArgs args) {

        this.throwIfClosed();

        if (logger.isDebugEnabled()) {
            args.traceOperation(logger, null, "request");
            logger.debug("\n  {}\n  {}\n  REQUEST", this, args);
        }

        final RntbdRequestRecord record = this.write(args);

        record.whenComplete((response, error) -> {

            args.traceOperation(logger, null, "requestComplete", response, error);

            if (error == null) {
                logger.debug("\n  [{}]\n  {}\n  request succeeded with response status: {}", this, args, response.getStatus());
            } else {
                logger.debug("\n  [{}]\n  {}\n  request failed due to ", this, args, error);
            }

        });

        return record;
    }

    @Override
    public String toString() {
        return RntbdObjectMapper.toString(this);
    }

    // endregion

    // region Privates

    private void releaseToPool(final Channel channel) {

        logger.debug("\n  [{}]\n  {}\n  RELEASE", this, channel);

        this.channelPool.release(channel).addListener(future -> {
            if (logger.isDebugEnabled()) {
                if (future.isSuccess()) {
                    logger.debug("\n  [{}]\n  {}\n  release succeeded", this, channel);
                } else {
                    logger.debug("\n  [{}]\n  {}\n  release failed due to {}", this, channel, future.cause());
                }
            }
        });
    }

    private void throwIfClosed() {
        checkState(!this.closed.get(), "%s is closed", this);
    }

    private RntbdRequestRecord write(final RntbdRequestArgs requestArgs) {

        final RntbdRequestRecord requestRecord = new RntbdRequestRecord(requestArgs, this.requestTimer);
        logger.debug("\n  [{}]\n  {}\n  WRITE", this, requestArgs);

        this.channelPool.acquire().addListener(connected -> {

            if (connected.isSuccess()) {

                requestArgs.traceOperation(logger, null, "write");
                final Channel channel = (Channel)connected.get();
                this.releaseToPool(channel);

                channel.write(requestRecord).addListener((ChannelFuture future) -> {
                    requestArgs.traceOperation(logger, null, "writeComplete", channel);
                });

                return;
            }

            final UUID activityId = requestArgs.activityId();
            final Throwable cause = connected.cause();

            if (connected.isCancelled()) {

                logger.debug("\n  [{}]\n  {}\n  write cancelled: {}", this, requestArgs, cause);
                requestRecord.cancel(true);

            } else {

                logger.debug("\n  [{}]\n  {}\n  write failed due to {} ", this, requestArgs, cause);
                final String reason = cause.getMessage();

                final GoneException goneException = new GoneException(
                    Strings.lenientFormat("failed to establish connection to %s: %s", this.remoteAddress, reason),
                    cause instanceof Exception ? (Exception)cause : new IOException(reason, cause),
                    ImmutableMap.of(HttpHeaders.ACTIVITY_ID, activityId.toString()),
                    requestArgs.replicaPath()
                );

                BridgeInternal.setRequestHeaders(goneException, requestArgs.serviceRequest().getHeaders());
                requestRecord.completeExceptionally(goneException);
            }
        });

        return requestRecord;
    }

    // endregion

    // region Types

    static final class JsonSerializer extends StdSerializer<RntbdServiceEndpoint> {

        public JsonSerializer() {
            super(RntbdServiceEndpoint.class);
        }

        @Override
        public void serialize(RntbdServiceEndpoint value, JsonGenerator generator, SerializerProvider provider)
            throws IOException {
            generator.writeStartObject();
            generator.writeNumberField("id", value.id);
            generator.writeBooleanField("isClosed", value.isClosed());
            generator.writeStringField("remoteAddress", value.remoteAddress.toString());
            generator.writeObjectField("channelPool", value.channelPool);
            generator.writeEndObject();
        }
    }

    public static final class Provider implements RntbdEndpoint.Provider {

        private static final Logger logger = LoggerFactory.getLogger(Provider.class);

        private final AtomicBoolean closed = new AtomicBoolean();
        private final Config config;
        private final ConcurrentHashMap<String, RntbdEndpoint> endpoints = new ConcurrentHashMap<>();
        private final NioEventLoopGroup eventLoopGroup;
        private final RntbdRequestTimer requestTimer;

        public Provider(final Options options, final SslContext sslContext) {

            checkNotNull(options, "options");
            checkNotNull(sslContext, "sslContext");

            final DefaultThreadFactory threadFactory = new DefaultThreadFactory("CosmosEventLoop", true);
            final int threadCount = Runtime.getRuntime().availableProcessors();
            final LogLevel wireLogLevel;

            if (logger.isTraceEnabled()) {
                wireLogLevel = LogLevel.TRACE;
            } else if (logger.isDebugEnabled()) {
                wireLogLevel = LogLevel.DEBUG;
            } else {
                wireLogLevel = null;
            }

            this.config = new Config(options, sslContext, wireLogLevel);
            this.requestTimer = new RntbdRequestTimer(config.requestTimeout());
            this.eventLoopGroup = new NioEventLoopGroup(threadCount, threadFactory);
        }

        @Override
        public void close() throws RuntimeException {

            if (this.closed.compareAndSet(false, true)) {

                this.requestTimer.close();

                for (final RntbdEndpoint endpoint : this.endpoints.values()) {
                    endpoint.close();
                }

                this.eventLoopGroup.shutdownGracefully(QUIET_PERIOD, this.config.shutdownTimeout(), TimeUnit.NANOSECONDS).addListener(future -> {
                    if (future.isSuccess()) {
                        logger.debug("\n  [{}]\n  closed endpoints", this);
                        return;
                    }
                    logger.error("\n  [{}]\n  failed to close endpoints due to ", this, future.cause());
                });
                return;
            }

            logger.debug("\n  [{}]\n  already closed", this);
        }

        @Override
        public Config config() {
            return this.config;
        }

        @Override
        public int count() {
            return this.endpoints.size();
        }

        @Override
        public RntbdEndpoint get(URI physicalAddress) {
            return endpoints.computeIfAbsent(physicalAddress.getAuthority(), authority ->
                new RntbdServiceEndpoint(config, eventLoopGroup, requestTimer, physicalAddress)
            );
        }

        @Override
        public Stream<RntbdEndpoint> list() {
            return this.endpoints.values().stream();
        }

        private void deleteEndpoint(final URI physicalAddress) {

            // TODO: DANOBLE: Utilize this method of tearing down unhealthy endpoints
            //  Specifically, ensure that this method is called when a Read/WriteTimeoutException occurs or a health
            //  check request fails. This perhaps likely requires a change to RntbdClientChannelPool.
            //  Links:
            //  https://msdata.visualstudio.com/CosmosDB/_workitems/edit/331552
            //  https://msdata.visualstudio.com/CosmosDB/_workitems/edit/331593

            checkNotNull(physicalAddress, "physicalAddress: %s", physicalAddress);

            final String authority = physicalAddress.getAuthority();
            final RntbdEndpoint endpoint = this.endpoints.remove(authority);

            if (endpoint != null) {
                endpoint.close();
            }
        }
    }

    // endregion
}
