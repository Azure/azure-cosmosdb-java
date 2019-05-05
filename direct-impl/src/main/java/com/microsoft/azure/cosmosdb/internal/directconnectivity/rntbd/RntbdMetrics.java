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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.base.Stopwatch;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

@JsonPropertyOrder({
    "elapsedTime", "requestCount", "responseCount", "errorResponseCount", "responseRate", "completionRate", "throughput"
})
public final class RntbdMetrics {

    // region Fields

    private final AtomicLong errorResponseCount = new AtomicLong();
    private final Stopwatch lifetime = Stopwatch.createStarted();
    private final AtomicLong requestCount = new AtomicLong();
    private final AtomicLong responseCount = new AtomicLong();

    // endregion

    // region Accessors

    public double getCompletionRate() {
        return (this.responseCount.get() - this.errorResponseCount.get()) / this.requestCount.get();
    }

    public double getElapsedTime() {

        final Duration elapsed = this.lifetime.elapsed();
        final long seconds = elapsed.getSeconds();
        final int fraction = elapsed.getNano();

        return Double.longBitsToDouble(seconds) + (1E-9D * fraction);
    }

    public long getErrorResponseCount() {
        return this.errorResponseCount.get();
    }

    @JsonIgnore()
    public Stopwatch getLifetime() {
        return this.lifetime;
    }

    public long getRequestCount() {
        return this.requestCount.get();
    }

    public long getResponseCount() {
        return this.responseCount.get();
    }

    public double getResponseRate() {
        return this.responseCount.get() / this.requestCount.get();
    }

    public double getThroughput() {
        return this.responseCount.get() / this.getElapsedTime();
    }

    // endregion

    // region Methods

    public final void incrementErrorResponseCount() {
        this.errorResponseCount.incrementAndGet();
    }

    public final void incrementRequestCount() {
        this.requestCount.incrementAndGet();
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
