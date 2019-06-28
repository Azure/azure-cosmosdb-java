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

import io.netty.channel.Channel;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.microsoft.azure.cosmosdb.internal.directconnectivity.rntbd.RntbdReporter.reportIssueUnless;
import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

final class RntbdClientChannelHealthChecker implements ChannelHealthChecker {

    // region Fields

    private static final Logger logger = LoggerFactory.getLogger(RntbdClientChannelHealthChecker.class);
    private static final long NANOS_PER_SECOND = 1_000_000_000L;

    // A channel will be declared healthy if data was read recently as defined by this value.
    private static final long recentReadWindow = NANOS_PER_SECOND;

    // A channel should not be declared unhealthy if a write succeeded recently. As such gaps between
    // Timestamps.lastChannelWrite and Timestamps.lastChannelRead lower than this value are ignored.
    // Guidance: The grace period should be large enough to accommodate the round trip time of the slowest server
    // request. Assuming 1s of network RTT, a 2 MB request, a 2 MB response, a connection that can sustain 1 MB/s
    // both ways, and a 5-second deadline at the server, 10 seconds should be enough.
    private static final long readHangGracePeriod = 10L * NANOS_PER_SECOND;

    // A channel will not be declared unhealthy if a write was attempted recently. As such gaps between
    // Timestamps.lastChannelWriteAttempt and Timestamps.lastChannelWrite lower than this value are ignored.
    // Guidance: The grace period should be large enough to accommodate slow writes. For example, a value of 2s requires
    // that the client can sustain data rates of at least 1 MB/s when writing 2 MB documents.
    private static final long writeHangGracePeriod = 2L * NANOS_PER_SECOND;

    // A channel is considered idle if:
    // idleConnectionTimeout > 0L && System.nanoTime() - Timestamps.lastChannelRead() >= idleConnectionTimeout
    private final long idleConnectionTimeout;

    // A channel will be declared unhealthy if the gap between Timestamps.lastChannelWrite and Timestamps.lastChannelRead
    // grows beyond this value.
    // Constraint: readDelayLimit > readHangGracePeriod
    private final long readDelayLimit;

    // A channel will be declared unhealthy if the gap between Timestamps.lastChannelWriteAttempt and Timestamps.lastChannelWrite
    // grows beyond this value.
    // Constraint: writeDelayLimit > writeHangGracePeriod
    private final long writeDelayLimit;

    // endregion

    // region Constructors

    public RntbdClientChannelHealthChecker(RntbdEndpoint.Config config) {

        this.idleConnectionTimeout = config.idleConnectionTimeout();

        this.readDelayLimit = config.receiveHangDetectionTime();
        checkArgument(this.readDelayLimit > readHangGracePeriod, "config.receiveHangDetectionTime: %s", this.readDelayLimit);

        this.writeDelayLimit = config.sendHangDetectionTime();
        checkArgument(this.writeDelayLimit > writeHangGracePeriod, "config.sendHangDetectionTime: %s", this.writeDelayLimit);
    }

    // endregion

    // region Methods

    public Future<Boolean> isHealthy(Channel channel) {

        checkNotNull(channel);

        RntbdRequestManager requestManager = channel.pipeline().get(RntbdRequestManager.class);
        Promise<Boolean> promise = channel.eventLoop().newPromise();

        if (requestManager == null) {
            reportIssueUnless(!channel.isActive(), logger, channel, "{} active with no request manager");
            return promise.setSuccess(Boolean.FALSE);
        }

        Timestamps timestamps = requestManager.timestamps();
        long currentTime = System.nanoTime();

        if (currentTime - timestamps.lastChannelRead() < recentReadWindow) {
            return promise.setSuccess(Boolean.TRUE); // because we recently received data
        }

        // Black hole detection, part 1:
        // Treat the channel as unhealthy if the gap between the last attempted write and the last successful write
        // grew beyond acceptable limits, unless a write was attempted recently. This is a sign of a hung write.

        if (timestamps.lastChannelWriteAttempt() - timestamps.lastChannelWrite() > this.writeDelayLimit && currentTime - timestamps.lastChannelWriteAttempt() > writeHangGracePeriod) {
            logger.warn("{} health check failed due to a hung write: {lastChannelWriteAttempt: {}, lastChannelWrite: {}, writeDelayLimit: {}}",
                channel, timestamps.lastChannelWriteAttempt(), timestamps.lastChannelWrite(), this.writeDelayLimit);
            return promise.setSuccess(Boolean.FALSE);
        }

        // Black hole detection, part 2:
        // Treat the connection as unhealthy if the gap between the last successful write and the last successful read
        // grew beyond acceptable limits, unless a write succeeded recently.

        if (timestamps.lastChannelWrite() - timestamps.lastChannelRead() > this.readDelayLimit && currentTime - timestamps.lastChannelWrite() > readHangGracePeriod) {
            logger.warn("{} health check failed due to response lag: {lastWriteTime: {}, lastReadTime: {}. readDelayLimit: {}}",
                channel, timestamps.lastChannelWrite(), timestamps.lastChannelRead(), this.readDelayLimit);
            return promise.setSuccess(Boolean.FALSE);
        }

        if (this.idleConnectionTimeout > 0L) {
            if (currentTime - timestamps.lastChannelRead() > this.idleConnectionTimeout) {
                return promise.setSuccess(Boolean.FALSE);
            }
        }

        channel.writeAndFlush(RntbdHealthCheckRequest.MESSAGE).addListener(completed -> {
            promise.setSuccess(completed.isSuccess() ? Boolean.TRUE : Boolean.FALSE);
        });

        return promise;
    }

    // endregion

    // region Types

    static final class Timestamps {

        private static final AtomicLongFieldUpdater<Timestamps> lastReadUpdater = newUpdater(Timestamps.class, "lastRead");
        private static final AtomicLongFieldUpdater<Timestamps> lastWriteUpdater = newUpdater(Timestamps.class, "lastWrite");
        private static final AtomicLongFieldUpdater<Timestamps> lastWriteAttemptUpdater = newUpdater(Timestamps.class, "lastWriteAttempt");

        private volatile long lastRead;
        private volatile long lastWrite;
        private volatile long lastWriteAttempt;

        public Timestamps() {
        }

        @SuppressWarnings("CopyConstructorMissesField")
        public Timestamps(Timestamps other) {
            lastReadUpdater.set(this, lastReadUpdater.get(other));
            lastWriteUpdater.set(this, lastWriteUpdater.get(other));
            lastWriteAttemptUpdater.set(this, lastWriteAttemptUpdater.get(other));
        }

        public void channelReadCompleted() {
            lastReadUpdater.set(this, System.nanoTime());
        }

        public void channelWriteAttempted() {
            lastWriteUpdater.set(this, System.nanoTime());
        }

        public void channelWriteCompleted() {
            lastWriteAttemptUpdater.set(this, System.nanoTime());
        }

        public long lastChannelRead() {
            return lastReadUpdater.get(this);
        }

        public long lastChannelWrite() {
            return lastWriteUpdater.get(this);
        }

        public long lastChannelWriteAttempt() {
            return lastWriteAttemptUpdater.get(this);
        }
    }

    // endregion
}
