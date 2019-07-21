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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.util.concurrent.atomic.AtomicInteger;

@JsonPropertyOrder({ "concurrentRequests", "requests", "responseErrors", "responseSuccesses", "completionRate", "responseRate" })
public class RntbdRequestMetrics extends RntbdMetrics {

    // region Fields

    private final AtomicInteger concurrentRequestCount = new AtomicInteger();
    private final DistributionSummary concurrentRequests;
    private final Timer requests;
    private final Timer responseErrors;
    private final Timer responseSuccesses;

    // endregion

    // region Constructors

    public RntbdRequestMetrics(final Class<?> cls, final long id) {

        super(cls, id);

        MeterRegistry registry = RntbdMetrics.registry();

        this.concurrentRequests = registry.summary(this.nameOf("concurrentRequests"));
        this.requests = registry.timer(this.nameOf("requests"));
        this.responseErrors = registry.timer(this.nameOf("responseErrors"));
        this.responseSuccesses = registry.timer(this.nameOf("responseSuccesses"));

        registry.gauge(this.nameOf("completionRate"), this, RntbdRequestMetrics::completionRate);
        registry.gauge(this.nameOf("responseRate"), this, RntbdRequestMetrics::responseRate);
    }

    // endregion

    // region Accessors

    @JsonProperty
    public Iterable<Measurement> concurrentRequests() {
        return this.concurrentRequests.measure();
    }

    /***
     * Computes the number of successful (non-error) responses received divided by the total number of completed
     * requests
     */
    @JsonProperty
    public double completionRate() {
        return this.responseSuccesses.count() / (double)this.requests.count();
    }

    @JsonProperty
    public Iterable<Measurement> requests() {
        return this.requests.measure();
    }

    @JsonProperty
    public Iterable<Measurement> responseErrors() {
        return this.responseErrors.measure();
    }

    /***
     * Computes the number of successful (non-error) responses received divided by the total number of requests sent
     */
    @JsonProperty
    public double responseRate() {
        return this.responseSuccesses.count() / (double)(this.requests.count() + this.concurrentRequests.count());
    }

    @JsonProperty
    public Iterable<Measurement> responseSuccesses() {
        return this.responseSuccesses.measure();
    }

    // endregion

    // region Methods

    public void markRequestStart() {
        this.concurrentRequests.record(this.concurrentRequestCount.incrementAndGet());
    }

    public void markRequestComplete(RntbdRequestRecord record) {
        record.stop(this.requests, record.isCompletedExceptionally() ? this.responseErrors : this.responseSuccesses);
        this.concurrentRequests.record(this.concurrentRequestCount.decrementAndGet());
    }

    // endregion
}
