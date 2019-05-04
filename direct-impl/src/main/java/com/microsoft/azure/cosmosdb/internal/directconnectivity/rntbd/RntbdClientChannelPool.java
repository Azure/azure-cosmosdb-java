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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class RntbdClientChannelPool extends FixedChannelPool {

    private static final Logger logger = LoggerFactory.getLogger(RntbdClientChannelPool.class);
    private final ConcurrentHashMap<ChannelId, Channel> atCapacity;
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
        this.atCapacity = new ConcurrentHashMap<>();
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
    protected boolean offerChannel(final Channel channel) {

        if (pendingRequestCount(channel) >= this.maxRequestsPerChannel) {
            this.atCapacity.put(channel.id(), channel);
            return true;
        }

        return super.offerChannel(channel);
    }

    /**
     * Poll a {@link Channel} out of internal storage to reuse it
     * <p>
     * Maintainers: Implementations of this method must be thread-safe.
     *
     * @return a value of {@code null}, if no {@link Channel} is ready to be reused
     */
    @Override
    protected Channel pollChannel() {

        final Channel channel = super.pollChannel();

        if (channel != null || this.atCapacity.isEmpty()) {
            return channel;
        }

        return this.atCapacity.search(100L, (id, value) -> {
            if (pendingRequestCount(value) < this.maxRequestsPerChannel) {
                this.atCapacity.remove(id);
                return value;
            }
            return null;
        });
    }

    private static int pendingRequestCount(final Channel channel) {
        return channel.pipeline().get(RntbdRequestManager.class).getPendingRequestCount();
    }

}
