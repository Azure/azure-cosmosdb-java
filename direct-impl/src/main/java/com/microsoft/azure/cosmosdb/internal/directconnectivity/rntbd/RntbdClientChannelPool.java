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
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.SimpleChannelPool;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.Promise;
import io.netty.util.internal.ThrowableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkState;
import static com.microsoft.azure.cosmosdb.internal.directconnectivity.rntbd.RntbdReporter.reportIssueUnless;

/**
 * {@link ChannelPool} implementation that enforces a maximum number of concurrent direct TCP Cosmos connections
 */
@JsonSerialize(using = RntbdClientChannelPool.JsonSerializer.class)
public final class RntbdClientChannelPool extends SimpleChannelPool {

    private static final TimeoutException ACQUISITION_TIMEOUT = ThrowableUtil.unknownStackTrace(
        new TimeoutException("Acquisition took longer than the configured maximum time"),
        RntbdClientChannelPool.class, "<init>(...)");

    private static final IllegalStateException POOL_CLOSED_ON_ACQUIRE = ThrowableUtil.unknownStackTrace(
        new IllegalStateException("RntbdClientChannelPool was closed"),
        RntbdClientChannelPool.class, "acquire0(...)");

    private static final IllegalStateException POOL_CLOSED_ON_RELEASE = ThrowableUtil.unknownStackTrace(
        new IllegalStateException("RntbdClientChannelPool was closed"),
        RntbdClientChannelPool.class, "release(...)");

    private static final IllegalStateException TOO_MANY_PENDING_ACQUISITIONS = ThrowableUtil.unknownStackTrace(
        new IllegalStateException("Too many outstanding acquire operations"),
        RntbdClientChannelPool.class, "acquire0(...)");

    private static final Logger logger = LoggerFactory.getLogger(RntbdClientChannelPool.class);

    private final long acquisitionTimeoutNanos;
    private final AtomicInteger acquiredChannelCount;
    private final AtomicInteger availableChannelCount;
    private final AtomicBoolean closed;
    private final EventExecutor executor;
    private final int maxChannels;
    private final int maxPendingAcquisitions;
    private final int maxRequestsPerChannel;

    // There is no need to worry about synchronization as everything that modified the queue or counts is done
    // by the above EventExecutor.
    private final Queue<AcquireTask> pendingAcquisitionQueue = new ArrayDeque<AcquireTask>();
    private final Runnable acquisitionTimeoutTask;
    private int pendingChannelAcquisitionCount;

    /**
     * Initializes a newly created {@link RntbdClientChannelPool} object
     *
     * @param bootstrap the {@link Bootstrap} that is used for connections
     * @param config    the {@link RntbdEndpoint.Config} that is used for the channel pool instance created
     */
    RntbdClientChannelPool(final Bootstrap bootstrap, final RntbdEndpoint.Config config) {

        super(bootstrap, new RntbdClientChannelHandler(config), ChannelHealthChecker.ACTIVE, true, true);

        this.executor = bootstrap.config().group().next();
        this.maxChannels = config.getMaxChannelsPerEndpoint();
        this.maxPendingAcquisitions = Integer.MAX_VALUE;
        this.maxRequestsPerChannel = config.getMaxRequestsPerChannel();

        this.availableChannelCount = new AtomicInteger();
        this.acquiredChannelCount = new AtomicInteger();
        this.closed = new AtomicBoolean();

        // TODO: DANOBLE: Add RntbdEndpoint.Config settings for acquisition timeout and acquisition timeout action
        //  Alternatively: drop acquisition timeout and acquisition timeout action
        //  Decision should be based on performance, reliability, and usability considerations

        final AcquisitionTimeoutAction acquisitionTimeoutAction = null;
        final long acquisitionTimeoutNanos = -1L;

        if (acquisitionTimeoutAction == null) {

            this.acquisitionTimeoutNanos = -1L;
            this.acquisitionTimeoutTask = null;

        } else {

            this.acquisitionTimeoutNanos = acquisitionTimeoutNanos;

            switch (acquisitionTimeoutAction) {
                case FAIL:
                    this.acquisitionTimeoutTask = new AcquireTimeoutTask(this) {
                        @Override
                        public void onTimeout(AcquireTask task) {
                            task.promise.setFailure(ACQUISITION_TIMEOUT);
                        }
                    };
                    break;
                case NEW:
                    this.acquisitionTimeoutTask = new AcquireTimeoutTask(this) {
                        @Override
                        public void onTimeout(AcquireTask task) {
                            // Increment the acquire count and get a new Channel by delegating to super.acquire
                            task.acquired();
                            RntbdClientChannelPool.super.acquire(task.promise);
                        }
                    };
                    break;
                default:
                    throw new Error();
            }
        }
    }

    // region Methods

    public boolean isClosed() {
        return this.closed.get();
    }

    @Override
    public Future<Channel> acquire(final Promise<Channel> promise) {

        this.throwIfClosed();

        try {
            if (this.executor.inEventLoop()) {
                this.acquire0(promise);
            } else {
                this.executor.execute(() -> this.acquire0(promise));
            }
        } catch (Throwable cause) {
            promise.setFailure(cause);
        }

        return promise;
    }

    public int acquiredChannelCount() {
        return acquiredChannelCount.get();
    }

    public int availableChannelCount() {
        return this.availableChannelCount.get();
    }

    @Override
    public void close() {
        if (this.closed.compareAndSet(false, true)) {
            if (this.executor.inEventLoop()) {
                this.close0();
            } else {
                this.executor.submit(this::close0).awaitUninterruptibly();
            }
        }
    }

    public int maxChannels() {
        return this.maxChannels;
    }

    public int maxRequestsPerChannel() {
        return this.maxRequestsPerChannel;
    }

    public int pendingChannelAcquisitionCount() {
        return this.pendingChannelAcquisitionCount;
    }

    @Override
    public Future<Void> release(final Channel channel, final Promise<Void> promise) {

        this.throwIfClosed();

        super.release(channel, this.executor.<Void>newPromise().addListener((FutureListener<Void>)future -> {

            checkState(this.executor.inEventLoop());

            if (this.isClosed()) {
                // Since the pool is closed, we have no choice but to close the channel
                promise.setFailure(POOL_CLOSED_ON_RELEASE);
                channel.close();
                return;
            }

            if (future.isSuccess()) {

                this.decrementAndRunTaskQueue();
                promise.setSuccess(null);

            } else {

                final Throwable cause = future.cause();

                if (!(cause instanceof IllegalArgumentException)) {
                    this.decrementAndRunTaskQueue();
                }

                promise.setFailure(cause);
            }
        }));

        return promise;
    }

    public SocketAddress remoteAddress() {
        return this.bootstrap().config().remoteAddress();
    }

    @Override
    public String toString() {
        return "RntbdClientChannelPool(" + RntbdObjectMapper.toJson(this) + ")";
    }

    /**
     * Offer a {@link Channel} back to the internal storage
     * <p>
     * Maintainers: Implementations of this method must be thread-safe.
     *
     * @param channel the {@link Channel} to return to internal storage
     * @return {@code true}, if the {@link Channel} could be added to internal storage; otherwise {@code false}
     */
    @Override
    protected boolean offerChannel(final Channel channel) {
        if (super.offerChannel(channel)) {
            this.availableChannelCount.incrementAndGet();
            return true;
        }
        return false;
    }

    /**
     * Poll a {@link Channel} out of internal storage to reuse it
     * <p>
     * Maintainers: Implementations of this method must be thread-safe and this type ensures thread safety by calling
     * this method serially on a single-threaded EventExecutor. As a result this method need not (and should not) be
     * synchronized.
     *
     * @return a value of {@code null}, if no {@link Channel} is ready to be reused
     * @see #acquire(Promise)
     */
    @Override
    protected Channel pollChannel() {

        final Channel first = super.pollChannel();

        if (first == null) {
            return null;
        }

        if (this.isClosed()) {
            return first;  // because this.close -> this.close0 -> super.close -> this.pollChannel
        }

        if (this.isInactiveOrServiceableChannel(first)) {
            return this.decrementAvailableChannelCountAndAccept(first);
        }

        super.offerChannel(first);  // because we need a non-null sentinel to stop the search for a channel

        for (Channel next = super.pollChannel(); next != first; super.offerChannel(next), next = super.pollChannel()) {
            if (this.isInactiveOrServiceableChannel(next)) {
                return this.decrementAvailableChannelCountAndAccept(next);
            }
        }

        super.offerChannel(first);  // because we choose not to check any channel more than once in a single call
        return null;
    }

    // endregion

    // region Privates

    private void acquire0(final Promise<Channel> promise) {

        checkState(this.executor.inEventLoop());

        if (this.isClosed()) {
            promise.setFailure(POOL_CLOSED_ON_ACQUIRE);
            return;
        }

        if (this.acquiredChannelCount.get() < this.maxChannels) {

            // We need to create a new promise to ensure the AcquireListener runs in the correct event loop

            checkState(this.acquiredChannelCount.get() >= 0);

            final AcquireListener l = new AcquireListener(this, promise);
            l.acquired();

            final Promise<Channel> p = this.executor.newPromise();
            p.addListener(l);

            super.acquire(p); // acquire an existing channel or create and acquire a new channel

        } else {

            if (this.pendingChannelAcquisitionCount >= this.maxPendingAcquisitions) {

                promise.setFailure(TOO_MANY_PENDING_ACQUISITIONS);

            } else {

                // Add a task to the pending acquisition queue and we'll satisfy this request later

                AcquireTask task = new AcquireTask(this, promise);

                if (this.pendingAcquisitionQueue.offer(task)) {

                    this.pendingChannelAcquisitionCount++;

                    if (acquisitionTimeoutTask != null) {
                        task.timeoutFuture = executor.schedule(acquisitionTimeoutTask, acquisitionTimeoutNanos, TimeUnit.NANOSECONDS);
                    }

                } else {
                    promise.setFailure(TOO_MANY_PENDING_ACQUISITIONS);
                }
            }

            checkState(this.pendingChannelAcquisitionCount > 0);
        }
    }

    private void close0() {

        checkState(this.executor.inEventLoop());

        for (; ; ) {
            final AcquireTask task = this.pendingAcquisitionQueue.poll();
            if (task == null) {
                break;
            }
            final ScheduledFuture<?> timeoutFuture = task.timeoutFuture;
            if (timeoutFuture != null) {
                timeoutFuture.cancel(false);
            }
            task.promise.setFailure(new ClosedChannelException());
        }

        this.acquiredChannelCount.set(0);
        this.availableChannelCount.set(0);
        this.pendingChannelAcquisitionCount = 0;

        // Ensure we dispatch this on another Thread as close0 will be called from the EventExecutor and we need
        // to ensure we will not block in an EventExecutor

        GlobalEventExecutor.INSTANCE.execute(RntbdClientChannelPool.super::close);
    }

    private void decrementAndRunTaskQueue() {

        final int currentCount = this.acquiredChannelCount.decrementAndGet();
        checkState(currentCount >= 0);

        // Run the pending acquisition tasks before notifying the original promise so that if the user tries to
        // acquire again from the ChannelFutureListener and the pendingChannelAcquisitionCount is greater than
        // maxPendingAcquisitions we may be able to run some pending tasks first and so allow to add more
        runTaskQueue();
    }

    private Channel decrementAvailableChannelCountAndAccept(final Channel first) {
        this.availableChannelCount.decrementAndGet();
        return first;
    }

    private boolean isInactiveOrServiceableChannel(final Channel channel) {

        if (!channel.isActive()) {
            return true;
        }

        final RntbdRequestManager requestManager = channel.pipeline().get(RntbdRequestManager.class);

        if (requestManager == null) {
            reportIssueUnless(!channel.isActive(), logger, this, "{} active with no request manager", channel);
            return true; // inactive
        }

        return requestManager.isServiceable(1 /* this.maxRequestsPerChannel */);
    }

    private void runTaskQueue() {

        while (this.acquiredChannelCount.get() < this.maxChannels) {

            final AcquireTask task = this.pendingAcquisitionQueue.poll();

            if (task == null) {
                break;
            }

            final ScheduledFuture<?> timeoutFuture = task.timeoutFuture;

            if (timeoutFuture != null) {
                timeoutFuture.cancel(false);
            }

            this.pendingChannelAcquisitionCount--;
            task.acquired();

            super.acquire(task.promise);
        }

        // We should never have negative values

        checkState(this.acquiredChannelCount.get() >= 0);
        checkState(this.pendingChannelAcquisitionCount >= 0);
    }

    private void throwIfClosed() {
        checkState(!this.isClosed(), "%s is closed", this);
    }

    // endregion

    // region Types

    private enum AcquisitionTimeoutAction {

        /**
         * Create a new connection when the timeout is detected.
         */
        NEW,

        /**
         * Fail the {@link Future} of the acquire call with a {@link TimeoutException}.
         */
        FAIL
    }

    private static class AcquireListener implements FutureListener<Channel> {

        private final Promise<Channel> originalPromise;
        private final RntbdClientChannelPool pool;
        private boolean acquired;

        AcquireListener(RntbdClientChannelPool pool, Promise<Channel> originalPromise) {
            this.originalPromise = originalPromise;
            this.pool = pool;
        }

        public void acquired() {
            if (this.acquired) {
                return;
            }
            this.pool.acquiredChannelCount.incrementAndGet();
            this.acquired = true;
        }

        @Override
        public void operationComplete(Future<Channel> future) {

            checkState(this.pool.executor.inEventLoop());

            if (this.pool.isClosed()) {
                if (future.isSuccess()) {
                    // Since the pool is closed, we have no choice but to close the channel
                    future.getNow().close();
                }
                this.originalPromise.setFailure(POOL_CLOSED_ON_ACQUIRE);
                return;
            }

            if (future.isSuccess()) {

                // Ensure that the channel is active and ready to receive requests
                // A Direct TCP channel is ready to receive requests when it:
                // * is active and
                // * has an RntbdContext
                // We use a health check request on a channel without an RntbdContext to force:
                // 1. SSL negotiation
                // 2. RntbdContextRequest -> RntbdContext
                // 3. RntbdHealthCheckRequest -> RntbdHealthCheck

                final Channel channel = future.getNow();
                checkState(channel.isActive());

                final RntbdRequestManager requestManager = channel.pipeline().get(RntbdRequestManager.class);
                checkState(requestManager != null);

                if (requestManager.hasRequestedRntbdContext()) {

                    this.originalPromise.setSuccess(channel);

                } else {

                    channel.writeAndFlush(RntbdHealthCheckRequest.MESSAGE).addListener(completed -> {
                        if (completed.isSuccess()) {
                            reportIssueUnless(requestManager.hasRequestedRntbdContext(), logger, channel, "context request did not complete");
                            this.originalPromise.setSuccess(channel);
                        } else {
                            logger.error("{} health check request failed due to ", channel, completed.cause());
                            channel.close().addListener(closed -> this.pool.release(channel));
                            this.originalPromise.setFailure(completed.cause());
                        }
                    });
                }

            } else {

                if (this.acquired) {
                    this.pool.decrementAndRunTaskQueue();
                } else {
                    this.pool.runTaskQueue();
                }

                this.originalPromise.setFailure(future.cause());
            }
        }
    }

    private static final class AcquireTask extends AcquireListener {

        // AcquireTask extends AcquireListener to reduce object creations and so GC pressure

        final long expireNanoTime;
        final Promise<Channel> promise;

        ScheduledFuture<?> timeoutFuture;

        AcquireTask(RntbdClientChannelPool pool, Promise<Channel> promise) {
            // We need to create a new promise to ensure the AcquireListener runs in the correct event loop
            super(pool, promise);
            this.promise = pool.executor.<Channel>newPromise().addListener(this);
            this.expireNanoTime = System.nanoTime() + pool.acquisitionTimeoutNanos;
        }
    }

    private static abstract class AcquireTimeoutTask implements Runnable {

        final RntbdClientChannelPool pool;

        public AcquireTimeoutTask(RntbdClientChannelPool pool) {
            this.pool = pool;
        }

        public abstract void onTimeout(AcquireTask task);

        @Override
        public final void run() {

            checkState(this.pool.executor.inEventLoop());
            final long nanoTime = System.nanoTime();

            for (; ; ) {
                AcquireTask task = this.pool.pendingAcquisitionQueue.peek();
                // Compare nanoTime as described in the System.nanoTime documentation
                // See:
                // * https://docs.oracle.com/javase/7/docs/api/java/lang/System.html#nanoTime()
                // * https://github.com/netty/netty/issues/3705
                if (task == null || nanoTime - task.expireNanoTime < 0) {
                    break;
                }
                this.pool.pendingAcquisitionQueue.remove();
                this.pool.pendingChannelAcquisitionCount--;
                this.onTimeout(task);
            }
        }
    }

    static final class JsonSerializer extends StdSerializer<RntbdClientChannelPool> {

        JsonSerializer() {
            super(RntbdClientChannelPool.class);
        }

        @Override
        public void serialize(RntbdClientChannelPool pool, JsonGenerator generator, SerializerProvider provider) throws IOException {
            generator.writeStartObject();
            generator.writeStringField("remoteAddress", pool.remoteAddress().toString());
            generator.writeNumberField("maxChannels", pool.maxChannels());
            generator.writeNumberField("maxRequestsPerChannel", pool.maxRequestsPerChannel());
            generator.writeObjectFieldStart("state");
            generator.writeBooleanField("isClosed", pool.isClosed());
            generator.writeNumberField("acquiredChannelCount", pool.acquiredChannelCount());
            generator.writeNumberField("availableChannelCount", pool.availableChannelCount());
            generator.writeNumberField("pendingChannelAcquisitionCount", pool.pendingChannelAcquisitionCount());
            generator.writeEndObject();
            generator.writeEndObject();
        }
    }

    // endregion
}
