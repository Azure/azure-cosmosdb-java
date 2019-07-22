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

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.collect.ImmutableList;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.RntbdTransportClient;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.config.NamingConvention;
import io.micrometer.core.instrument.dropwizard.DropwizardConfig;
import io.micrometer.core.instrument.dropwizard.DropwizardMeterRegistry;
import io.micrometer.core.instrument.util.HierarchicalNameMapper;
import io.micrometer.core.lang.Nullable;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@JsonPropertyOrder({
    "concurrentRequests", "requests", "responseErrors", "responseSuccesses", "completionRate", "responseRate",
    "channelsAcquired", "channelsAvailable", "requestQueueLength", "usedDirectMemory", "usedHeapMemory"
})
public final class RntbdMetrics {

    // region Fields

    private static final CompositeMeterRegistry registry = new CompositeMeterRegistry();
    private static final String prefix = "cosmos.directTcp.";
    private static MeterRegistry consoleLoggingRegistry;

    private final DistributionSummary channelsAcquired;
    private final DistributionSummary channelsAvailable;
    private final AtomicInteger concurrentRequestCount = new AtomicInteger();
    private final DistributionSummary concurrentRequests;
    private final RntbdEndpoint endpoint;
    private final DistributionSummary requestQueueLength;
    private final Timer requests;
    private final Timer responseErrors;
    private final Timer responseSuccesses;
    private final List<Tag> tags;
    private final DistributionSummary usedDirectMemory;
    private final DistributionSummary usedHeapMemory;

    static {
        if (Boolean.getBoolean("cosmos.directTcp.consoleMetricsReporter.enabled")) {
            RntbdMetrics.add(RntbdMetrics.consoleLoggingRegistry());
        }
    }

    // endregion

    // region Constructors

    public RntbdMetrics(RntbdTransportClient client, RntbdEndpoint endpoint) {

        this.tags = ImmutableList.of(client.tag(), endpoint.tag());

        this.concurrentRequests = registry.summary(nameOf("concurrentRequests"), tags);
        this.requests = registry.timer(nameOf("requests"), tags);
        this.responseErrors = registry.timer(nameOf("responseErrors"), tags);
        this.responseSuccesses = registry.timer(nameOf("responseSuccesses"), tags);

        this.channelsAcquired = registry.summary(nameOf("channelsAcquired"), tags);
        this.channelsAvailable = registry.summary(nameOf("channelsAvailable"), tags);
        this.requestQueueLength = registry.summary(nameOf("requestQueueLength"), tags);
        this.usedDirectMemory = registry.summary(nameOf("usedDirectMemory"), tags);
        this.usedHeapMemory = registry.summary(nameOf("usedHeapMemory"), tags);

        registry.gauge(nameOf("completionRate"), tags, this, RntbdMetrics::completionRate);
        registry.gauge(nameOf("responseRate"), tags, this, RntbdMetrics::responseRate);

        this.endpoint = endpoint;
    }

    // endregion

    // region Accessors

    public static void add(MeterRegistry registry) {
        RntbdMetrics.registry.add(registry);
    }

    @JsonProperty
    public Iterable<Measurement> channelsAcquired() {
        return this.channelsAcquired.measure();
    }

    @JsonProperty
    public Iterable<Measurement> channelsAvailable() {
        return this.channelsAvailable.measure();
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
    public Iterable<Measurement> concurrentRequests() {
        return this.concurrentRequests.measure();
    }

    public static synchronized MeterRegistry consoleLoggingRegistry() {

        if (consoleLoggingRegistry == null) {

            MetricRegistry dropwizardRegistry = new MetricRegistry();

            ConsoleReporter consoleReporter = ConsoleReporter
                .forRegistry(dropwizardRegistry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();

            consoleReporter.start(1, TimeUnit.SECONDS);

            DropwizardConfig dropwizardConfig = new DropwizardConfig() {

                @Override
                public String get(@Nullable String key) {
                    return null;
                }

                @Override
                public String prefix() {
                    return "console";
                }

            };

            consoleLoggingRegistry = new DropwizardMeterRegistry(dropwizardConfig, dropwizardRegistry, HierarchicalNameMapper.DEFAULT, Clock.SYSTEM) {
                @Override
                @Nullable
                protected Double nullGaugeValue() {
                    return null;
                }
            };

            consoleLoggingRegistry.config().namingConvention(NamingConvention.dot);
        }

        return consoleLoggingRegistry;
    }

    @JsonProperty
    public Iterable<Measurement> requestQueueLength() {
        return this.requestQueueLength.measure();
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

    @JsonProperty
    public Iterable<Tag> tags() {
        return this.tags;
    }

    @JsonProperty
    public Iterable<Measurement> usedDirectMemory() {
        return this.usedDirectMemory.measure();
    }

    @JsonProperty
    public Iterable<Measurement> usedHeapMemory() {
        return this.usedHeapMemory.measure();
    }

    // endregion

    // region Methods

    public void markRequestComplete(RntbdRequestRecord record) {
        record.stop(this.requests, record.isCompletedExceptionally() ? this.responseErrors : this.responseSuccesses);
        this.concurrentRequests.record(this.concurrentRequestCount.decrementAndGet());
        this.takeChannelPoolMeasurements();
    }

    public void markRequestStart() {
        this.concurrentRequests.record(this.concurrentRequestCount.incrementAndGet());
        this.takeChannelPoolMeasurements();
    }

    @Override
    public String toString() {
        return RntbdObjectMapper.toString(this);
    }

    // endregion

    // region Private

    private static String nameOf(final String member) {
        return prefix + member;
    }

    private void takeChannelPoolMeasurements() {
        this.channelsAcquired.record(this.endpoint.acquiredChannels());
        this.channelsAvailable.record(this.endpoint.availableChannels());
        this.requestQueueLength.record(this.endpoint.requestQueueLength());
        this.usedDirectMemory.record(this.endpoint.usedDirectMemory());
        this.usedHeapMemory.record(this.endpoint.usedHeapMemory());
    }

    // endregion
}
