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
 * {@link ChannelPool} implementation that takes another {@link ChannelPool} implementation and enforce a maximum
 * number of concurrent connections.
 */
public final class RntbdClientChannelPool2 extends SimpleChannelPool {

    private static final IllegalStateException FULL_EXCEPTION = ThrowableUtil.unknownStackTrace(
        new IllegalStateException("Too many outstanding acquire operations"),
        RntbdClientChannelPool2.class, "acquire0(...)");

    private static final IllegalStateException POOL_CLOSED_ON_ACQUIRE_EXCEPTION = ThrowableUtil.unknownStackTrace(
        new IllegalStateException("FixedChannelPool was closed"),
        RntbdClientChannelPool2.class, "acquire0(...)");

    private static final IllegalStateException POOL_CLOSED_ON_RELEASE_EXCEPTION = ThrowableUtil.unknownStackTrace(
        new IllegalStateException("FixedChannelPool was closed"),
        RntbdClientChannelPool2.class, "release(...)");

    private static final TimeoutException TIMEOUT_EXCEPTION = ThrowableUtil.unknownStackTrace(
        new TimeoutException("Acquire operation took longer then configured maximum time"),
        RntbdClientChannelPool2.class, "<init>(...)");

    private static final Logger logger = LoggerFactory.getLogger(RntbdClientChannelPool2.class);

    private final long acquireTimeoutNanos;
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
    private final Runnable timeoutTask;
    private int pendingAcquisitionCount;

    /**
     * Initializes a newly created {@link RntbdClientChannelPool2} object
     *
     * @param bootstrap the {@link Bootstrap} that is used for connections
     * @param config    the {@link RntbdEndpoint.Config} that is used for the channel pool instance created
     */
    RntbdClientChannelPool2(final Bootstrap bootstrap, final RntbdEndpoint.Config config) {

        super(bootstrap, new RntbdClientChannelHandler(config), ChannelHealthChecker.ACTIVE, true, true);

        this.executor = bootstrap.config().group().next();
        this.maxChannels = config.getMaxChannelsPerEndpoint();
        this.maxPendingAcquisitions = Integer.MAX_VALUE;
        this.maxRequestsPerChannel = config.getMaxRequestsPerChannel();

        this.acquiredChannelCount = new AtomicInteger();
        this.availableChannelCount = new AtomicInteger();
        this.closed = new AtomicBoolean();

        this.acquireTimeoutNanos = -1;
        this.timeoutTask = null;
    }

    // region Methods

    @Override
    public Future<Channel> acquire(final Promise<Channel> promise) {

        this.throwIfClosed();

        try {
            if (executor.inEventLoop()) {
                acquire0(promise);
            } else {
                executor.execute(() -> acquire0(promise));
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
            if (executor.inEventLoop()) {
                close0();
            } else {
                executor.submit(this::close0).awaitUninterruptibly();
            }
        }
    }

    public int maxChannels() {
        return this.maxChannels;
    }

    public int maxRequestsPerChannel() {
        return this.maxRequestsPerChannel;
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

    public int pendingAcquisitionCount() {
        return this.pendingAcquisitionCount;
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

        if (this.closed.get()) {
            return first;  // because we're being called following a call to close (from super.close)
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

    @Override
    public Future<Void> release(final Channel channel, final Promise<Void> promise) {

        this.throwIfClosed();
        final Promise<Void> p = executor.newPromise();

        super.release(channel, p.addListener((FutureListener<Void>)future -> {

            checkState(executor.inEventLoop());

            if (closed.get()) {
                // Since the pool is closed, we have no choice but to close the channel
                promise.setFailure(POOL_CLOSED_ON_RELEASE_EXCEPTION);
                channel.close();
                return;
            }

            if (future.isSuccess()) {

                decrementAndRunTaskQueue();
                promise.setSuccess(null);

            } else {

                Throwable cause = future.cause();

                if (!(cause instanceof IllegalArgumentException)) {
                    decrementAndRunTaskQueue();
                }

                promise.setFailure(future.cause());
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

    // endregion

    // region Privates

    private void acquire0(final Promise<Channel> promise) {

        assert executor.inEventLoop();

        if (this.closed.get()) {
            promise.setFailure(POOL_CLOSED_ON_ACQUIRE_EXCEPTION);
            return;
        }
        if (acquiredChannelCount.get() < this.maxChannels) {
            assert acquiredChannelCount.get() >= 0;
            // We need to create a new promise as we need to ensure the AcquireListener runs in the correct
            // EventLoop
            Promise<Channel> p = executor.newPromise();
            AcquireListener l = new AcquireListener(promise);
            l.acquired();
            p.addListener(l);
            super.acquire(p);
        } else {
            if (pendingAcquisitionCount >= maxPendingAcquisitions) {
                promise.setFailure(FULL_EXCEPTION);
            } else {
                AcquireTask task = new AcquireTask(promise);
                if (pendingAcquisitionQueue.offer(task)) {
                    ++pendingAcquisitionCount;

                    if (timeoutTask != null) {
                        task.timeoutFuture = executor.schedule(timeoutTask, acquireTimeoutNanos, TimeUnit.NANOSECONDS);
                    }
                } else {
                    promise.setFailure(FULL_EXCEPTION);
                }
            }

            assert pendingAcquisitionCount > 0;
        }
    }

    private void close0() {

        this.availableChannelCount.set(0);

        for (; ; ) {
            AcquireTask task = pendingAcquisitionQueue.poll();
            if (task == null) {
                break;
            }
            ScheduledFuture<?> f = task.timeoutFuture;
            if (f != null) {
                f.cancel(false);
            }
            task.promise.setFailure(new ClosedChannelException());
        }

        this.acquiredChannelCount.set(0);
        this.pendingAcquisitionCount = 0;

        // Ensure we dispatch this on another Thread as close0 will be called from the EventExecutor and we need
        // to ensure we will not block in a EventExecutor

        GlobalEventExecutor.INSTANCE.execute(new Runnable() {
            @Override
            public void run() {
                RntbdClientChannelPool2.super.close();
            }
        });
    }

    private void decrementAndRunTaskQueue() {
        // We should never have a negative value.
        int currentCount = acquiredChannelCount.decrementAndGet();
        assert currentCount >= 0;

        // Run the pending acquire tasks before notify the original promise so if the user would
        // try to acquire again from the ChannelFutureListener and the pendingAcquireCount is >=
        // maxPendingAcquires we may be able to run some pending tasks first and so allow to add
        // more.
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

        return requestManager.isServiceable(this.maxRequestsPerChannel);
    }

    private void runTaskQueue() {

        while (acquiredChannelCount.get() < this.maxChannels) {

            AcquireTask task = pendingAcquisitionQueue.poll();

            if (task == null) {
                break;
            }

            // Cancel the timeout if one was scheduled
            ScheduledFuture<?> timeoutFuture = task.timeoutFuture;

            if (timeoutFuture != null) {
                timeoutFuture.cancel(false);
            }

            --pendingAcquisitionCount;
            task.acquired();

            super.acquire(task.promise);
        }

        // We should never have a negative value.
        assert pendingAcquisitionCount >= 0;
        assert acquiredChannelCount.get() >= 0;
    }

    private void throwIfClosed() {
        checkState(!this.closed.get(), "%s is closed", this);
    }

    // endregion

    // region Types

    public enum AcquireTimeoutAction {
        /**
         * Create a new connection when the timeout is detected.
         */
        NEW,

        /**
         * Fail the {@link Future} of the acquire call with a {@link TimeoutException}.
         */
        FAIL
    }

    private class AcquireListener implements FutureListener<Channel> {

        private final Promise<Channel> originalPromise;
        boolean acquired;

        AcquireListener(Promise<Channel> originalPromise) {
            this.originalPromise = originalPromise;
        }

        public void acquired() {
            if (acquired) {
                return;
            }
            acquiredChannelCount.incrementAndGet();
            acquired = true;
        }

        @Override
        public void operationComplete(Future<Channel> future) throws Exception {

            assert executor.inEventLoop();

            if (closed.get()) {
                if (future.isSuccess()) {
                    // Since the pool is closed, we have no choice but to close the channel
                    future.getNow().close();
                }
                originalPromise.setFailure(POOL_CLOSED_ON_ACQUIRE_EXCEPTION);
                return;
            }

            if (future.isSuccess()) {
                originalPromise.setSuccess(future.getNow());
            } else {
                if (acquired) {
                    decrementAndRunTaskQueue();
                } else {
                    runTaskQueue();
                }

                originalPromise.setFailure(future.cause());
            }
        }
    }

    // AcquireTask extends AcquireListener to reduce object creations and so GC pressure

    private final class AcquireTask extends AcquireListener {

        final long expireNanoTime = System.nanoTime() + acquireTimeoutNanos;
        final Promise<Channel> promise;
        ScheduledFuture<?> timeoutFuture;

        AcquireTask(Promise<Channel> promise) {
            super(promise);
            // We need to create a new promise as we need to ensure the AcquireListener runs in the correct
            // EventLoop.
            this.promise = executor.<Channel>newPromise().addListener(this);
        }
    }

    // endregion
}
