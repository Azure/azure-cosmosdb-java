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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelId;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public final class RntbdClientChannelPool extends FixedChannelPool {

    private static final Logger logger = LoggerFactory.getLogger(RntbdClientChannelPool.class);
    private final ConcurrentHashMap<ChannelId, Channel> atCapacity;
    private final AtomicInteger availableChannelCount;
    private final AtomicBoolean closed;
    private final int maxRequestsPerChannel;

    /**
     * Creates a new instance using the {@link ChannelHealthChecker#ACTIVE}
     *
     * @param bootstrap the {@link Bootstrap} that is used for connections
     * @param config    the {@link RntbdEndpoint.Config} that is used for the channel pool instance created
     */
    RntbdClientChannelPool(final Bootstrap bootstrap, final RntbdEndpoint.Config config) {

        super(bootstrap, new RntbdClientChannelHandler(config), ChannelHealthChecker.ACTIVE, null,
            -1L, config.getMaxChannelsPerEndpoint(), Integer.MAX_VALUE, true
        );

        this.maxRequestsPerChannel = config.getMaxRequestsPerChannel();
        this.availableChannelCount = new AtomicInteger();
        this.atCapacity = new ConcurrentHashMap<>();
        this.closed = new AtomicBoolean();
    }

    public int availableChannelCount() {
        return this.availableChannelCount.get();
    }

    @Override
    public void close() {

        if (this.closed.compareAndSet(false, true)) {

            if (!this.atCapacity.isEmpty()) {

                for (Channel channel : this.atCapacity.values()) {
                    super.offerChannel(channel);
                }

                this.atCapacity.clear();
            }

            this.availableChannelCount.set(0);
            super.close();
        }
    }

    /**
     * Poll a {@link Channel} out of internal storage to reuse it
     * <p>
     * Maintainers: Implementations of this method must be thread-safe.
     *
     * @return a value of {@code null}, if no {@link Channel} is ready to be reused
     */
    @Override
    protected synchronized Channel pollChannel() {

        final Channel channel = super.pollChannel();

        if (channel != null) {
            this.availableChannelCount.decrementAndGet();
            return channel;
        }

        if (this.atCapacity.isEmpty()) {
            return null;
        }

        return this.atCapacity.search(Long.MAX_VALUE, (id, value) -> {
            if (pendingRequestCount(value) < this.maxRequestsPerChannel) {
                this.availableChannelCount.decrementAndGet();
                this.atCapacity.remove(id);
                return value;
            }
            return null;
        });
    }

    /**
     * Offer a {@link Channel} back to the internal storage
     * <p>
     * Maintainers: Implementations of this method needs must be thread-safe.
     *
     * @param channel the {@link Channel} to return to internal storage
     * @return {@code true}, if the {@link Channel} could be added to internal storage; otherwise {@code false}
     */
    @Override
    protected synchronized boolean offerChannel(final Channel channel) {

        checkArgument(channel.isActive(), "%s inactive", channel);
        final boolean offered;

        if (pendingRequestCount(channel) >= this.maxRequestsPerChannel) {
            this.atCapacity.put(channel.id(), channel);
            offered = true;
        } else {
            offered = super.offerChannel(channel);
        }

        if (offered) {
            this.availableChannelCount.incrementAndGet();
        }

        return offered;
    }

    private static int pendingRequestCount(final Channel channel) {
        return channel.pipeline().get(RntbdRequestManager.class).getPendingRequestCount();
    }
}
