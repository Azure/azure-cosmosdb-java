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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.net.PercentEscaper;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.RntbdTransportClient;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.config.NamingConvention;
import io.micrometer.core.instrument.dropwizard.DropwizardConfig;
import io.micrometer.core.instrument.dropwizard.DropwizardMeterRegistry;
import io.micrometer.core.instrument.util.HierarchicalNameMapper;
import io.micrometer.core.lang.Nullable;

import java.util.concurrent.TimeUnit;

@SuppressWarnings("UnstableApiUsage")
@JsonPropertyOrder({
    "tags", "concurrentRequests", "requests", "responseErrors", "responseSuccesses", "completionRate", "responseRate",
    "channelsAcquired", "channelsAvailable", "requestQueueLength", "usedDirectMemory", "usedHeapMemory"
})
public final class RntbdMetrics {

    // region Fields

    private static final PercentEscaper escaper = new PercentEscaper("_-", false);
    private static final CompositeMeterRegistry registry = new CompositeMeterRegistry();

    private static final String prefix = "cosmos.directTcp.";
    private static MeterRegistry consoleLoggingRegistry;

    private final RntbdTransportClient transportClient;
    private final RntbdEndpoint endpoint;

    private final Timer requests;
    private final Timer responseErrors;
    private final Timer responseSuccesses;
    private final Tags tags;

    static {
        int period = Integer.getInteger("cosmos.monitoring.consoleLogging.period", 0);
        if (period > 0) {
            RntbdMetrics.add(RntbdMetrics.consoleLoggingRegistry(period));
        }
    }

    // endregion

    // region Constructors

    public RntbdMetrics(RntbdTransportClient client, RntbdEndpoint endpoint) {

        this.transportClient = client;
        this.endpoint = endpoint;

        this.tags = Tags.of(client.tag(), endpoint.tag());
        this.requests = registry.timer(nameOf("requests"), tags);
        this.responseErrors = registry.timer(nameOf("responseErrors"), tags);
        this.responseSuccesses = registry.timer(nameOf("responseSuccesses"), tags);

        Gauge.builder(nameOf("endpoints"), client, RntbdTransportClient::endpointCount)
             .description("endpoint count")
             .tag(client.tag().getKey(), client.tag().getValue())
             .register(registry);

        Gauge.builder(nameOf("endpointsEvicted"), client, RntbdTransportClient::endpointEvictionCount)
             .description("endpoint eviction count")
             .tag(client.tag().getKey(), client.tag().getValue())
             .register(registry);

        Gauge.builder(nameOf("concurrentRequests"), endpoint, RntbdEndpoint::concurrentRequests)
             .description("executing or queued request count")
             .tags(this.tags)
             .register(registry);

        Gauge.builder(nameOf("requestQueueLength"), endpoint, RntbdEndpoint::requestQueueLength)
            .description("queued request count")
            .tags(this.tags)
            .register(registry);

        Gauge.builder(nameOf("channelsAcquired"), endpoint, RntbdEndpoint::channelsAcquired)
             .description("acquired channel count")
             .tags(this.tags)
             .register(registry);

        Gauge.builder(nameOf("channelsAvailable"), endpoint, RntbdEndpoint::channelsAvailable)
             .description("available channel count")
             .tags(this.tags)
             .register(registry);

        Gauge.builder(nameOf("usedDirectMemory"), endpoint, x -> x.usedDirectMemory())
             .description("Java direct memory usage")
             .baseUnit("bytes")
             .tags(this.tags)
             .register(registry);

        Gauge.builder(nameOf("usedHeapMemory"), endpoint, x -> x.usedHeapMemory())
             .description("Java heap memory usage")
             .baseUnit("MiB")
             .tags(this.tags)
             .register(registry);
    }

    // endregion

    // region Accessors

    @JsonIgnore
    private static synchronized MeterRegistry consoleLoggingRegistry(int period) {

        if (consoleLoggingRegistry == null) {

            MetricRegistry dropwizardRegistry = new MetricRegistry();

            ConsoleReporter consoleReporter = ConsoleReporter
                .forRegistry(dropwizardRegistry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();

            consoleReporter.start(period, TimeUnit.SECONDS);

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
    public int channelsAcquired() {
        return this.endpoint.channelsAcquired();
    }

    @JsonProperty
    public int channelsAvailable() {
        return this.endpoint.channelsAvailable();
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
    public long concurrentRequests() {
        return this.endpoint.concurrentRequests();
    }

    @JsonProperty
    public int endpoints() {
        return this.transportClient.endpointCount();
    }

    @JsonProperty
    public int requestQueueLength() {
        return this.endpoint.requestQueueLength();
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
        return this.responseSuccesses.count() / (double)(this.requests.count() + this.endpoint.concurrentRequests());
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
    public long usedDirectMemory() {
        return this.endpoint.usedDirectMemory();
    }

    @JsonProperty
    public long usedHeapMemory() {
        return this.endpoint.usedHeapMemory();
    }

    // endregion

    // region Methods

    public static void add(MeterRegistry registry) {
        RntbdMetrics.registry.add(registry);
    }

    public void markComplete(RntbdRequestRecord record) {
        record.stop(this.requests, record.isCompletedExceptionally() ? this.responseErrors : this.responseSuccesses);
    }

    public static String escape(String value) {
        return escaper.escape(value);
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

    // endregion
}
