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

import com.google.common.collect.ImmutableMap;
import com.microsoft.azure.cosmosdb.BridgeInternal;
import com.microsoft.azure.cosmosdb.internal.HttpConstants;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.GoneException;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.RntbdTransportClient.Config;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.ServiceUnavailableException;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.StoreResponse;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static com.microsoft.azure.cosmosdb.internal.directconnectivity.RntbdTransportClient.Endpoint;
import static com.microsoft.azure.cosmosdb.internal.directconnectivity.RntbdTransportClient.Metrics;

public class RntbdServiceEndpoint implements Endpoint {

    private static final String className = RntbdServiceEndpoint.class.getCanonicalName();
    private static final AtomicLong instanceCount = new AtomicLong();
    private static final Logger logger = LoggerFactory.getLogger(RntbdServiceEndpoint.className);

    private final RntbdClientChannelPool channelPool;
    private final Metrics metrics;
    private final String name;
    private final SocketAddress remoteAddress;
    private final RntbdRequestTimer requestTimer;

    public RntbdServiceEndpoint(
        final Config config, final NioEventLoopGroup group, final RntbdRequestTimer timer, final URI physicalAddress
    ) {

        final Bootstrap bootstrap = new Bootstrap()
            .channel(NioSocketChannel.class)
            .group(group)
            .option(ChannelOption.AUTO_READ, true)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getConnectionTimeout())
            .option(ChannelOption.SO_KEEPALIVE, true)
            .remoteAddress(physicalAddress.getHost(), physicalAddress.getPort());

        this.name = RntbdServiceEndpoint.className + '-' + instanceCount.incrementAndGet();
        this.channelPool = new RntbdClientChannelPool(bootstrap, config);
        this.remoteAddress = bootstrap.config().remoteAddress();
        this.metrics = new Metrics();
        this.requestTimer = timer;
    }

    @Override
    public void close() {
        this.channelPool.close();
    }

    public CompletableFuture<StoreResponse> request(final RntbdRequestArgs args) {

        if (logger.isDebugEnabled()) {
            args.traceOperation(logger, null, "request");
            logger.debug("\n  {}\n  {}\n  REQUEST", this, args);
        }

        final CompletableFuture<StoreResponse> responseFuture = this.write(args);
        this.metrics.incrementRequestCount();

        return responseFuture.whenComplete((response, error) -> {

            args.traceOperation(logger, null, "requestComplete", response, error);
            assert (response == null && error != null) || (response != null && error == null);
            this.metrics.incrementResponseCount();

            if (error != null) {
                this.metrics.incrementRequestFailureCount();
            }

            if (logger.isDebugEnabled()) {
                if (error == null) {
                    final int status = response.getStatus();
                    logger.debug("\n  {}\n  {}\n  request succeeded with response status: {}", this, args, status);
                } else {
                    logger.debug("\n  {}\n  {}\n  request failed due to {}", this, args, error);
                }
            }
        });
    }

    @Override
    public String toString() {
        return '[' + this.name + '(' + this.remoteAddress + ", acquiredChannelCount: "
            + this.channelPool.acquiredChannelCount() + ", " + this.metrics
            + ")]";
    }

    private void releaseToPool(final Channel channel) {

        logger.debug("\n  {}\n  {}\n  RELEASE", this, channel);

        this.channelPool.release(channel).addListener(future -> {
            if (logger.isDebugEnabled()) {
                if (future.isSuccess()) {
                    logger.debug("\n  {}\n  {}\n  release succeeded", this, channel);
                } else {
                    logger.debug("\n  {}\n  {}\n  release failed due to {}", this, channel, future.cause());
                }
            }
        });
    }

    private CompletableFuture<StoreResponse> write(final RntbdRequestArgs requestArgs) {

        final CompletableFuture<StoreResponse> responseFuture = new CompletableFuture<>();
        logger.debug("\n  {}\n  {}\n  WRITE", this, requestArgs);

        this.channelPool.acquire().addListener(connected -> {

            if (connected.isSuccess()) {

                final Channel channel = (Channel)connected.get();
                this.releaseToPool(channel);

                if (!channel.isOpen()) {
                    String message = String.format("%s request failed because channel closed unexpectedly", requestArgs);
                    GoneException error = new GoneException(message, null, null, requestArgs.getPhysicalAddress());
                    this.metrics.incrementRequestFailureCount();
                    responseFuture.completeExceptionally(error);
                    return;
                }

                final RntbdRequestManager requestManager = channel.pipeline().get(RntbdRequestManager.class);
                requestManager.createPendingRequest(requestArgs, this.requestTimer, responseFuture);
                requestArgs.traceOperation(logger, null, "write");

                channel.write(requestArgs).addListener((ChannelFuture future) -> {
                    if (!future.isSuccess()) {
                        this.metrics.incrementRequestFailureCount();
                    }
                    requestArgs.traceOperation(logger, null, "writeComplete", channel);
                });

                return;
            }

            final UUID activityId = requestArgs.getActivityId();
            final Throwable cause = connected.cause();

            if (connected.isCancelled()) {

                logger.debug("\n  {}\n  {}\n  write cancelled: {}", this, requestArgs, cause);
                responseFuture.cancel(true);

            } else {

                logger.debug("\n  {}\n  {}\n  write failed due to {} ", this, requestArgs, cause);
                final String reason = cause.getMessage();

                final GoneException goneException = new GoneException(
                    String.format("failed to establish connection to %s: %s", this.remoteAddress, reason),
                    cause instanceof Exception ? (Exception)cause : new IOException(reason, cause),
                    ImmutableMap.of(HttpConstants.HttpHeaders.ACTIVITY_ID, activityId.toString()),
                    requestArgs.getReplicaPath()
                );

                BridgeInternal.setRequestHeaders(goneException, requestArgs.getServiceRequest().getHeaders());
                responseFuture.completeExceptionally(goneException);
            }
        });

        return responseFuture;
    }
}
