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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.google.common.base.Stopwatch;
import com.microsoft.azure.cosmosdb.rx.internal.RxDocumentServiceRequest;
import io.micrometer.core.instrument.Timer;
import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import java.math.BigDecimal;
import java.net.URI;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.micrometer.core.instrument.Timer.Sample;

@JsonPropertyOrder({
    "transportRequestId", "activityId", "origin", "replicaPath", "operationType", "resourceType", "timeCreated",
    "lifetime"
})
public final class RntbdRequestArgs {

    private static final AtomicLong instanceCount = new AtomicLong();

    private final Sample sample;
    private final UUID activityId;
    private final OffsetDateTime timeCreated;
    private final long nanoTimeCreated;
    private final Stopwatch lifetime;
    private final String origin;
    private final URI physicalAddress;
    private final String replicaPath;
    private final RxDocumentServiceRequest serviceRequest;
    private final long transportRequestId;

    public RntbdRequestArgs(final RxDocumentServiceRequest serviceRequest, final URI physicalAddress) {
        this.sample = Timer.start();
        this.activityId = UUID.fromString(serviceRequest.getActivityId());
        this.timeCreated = OffsetDateTime.now();
        this.nanoTimeCreated = System.nanoTime();
        this.lifetime = Stopwatch.createStarted();
        this.origin = physicalAddress.getScheme() + "://" + physicalAddress.getAuthority();
        this.physicalAddress = physicalAddress;
        this.replicaPath = StringUtils.stripEnd(physicalAddress.getPath(), "/");
        this.serviceRequest = serviceRequest;
        this.transportRequestId = instanceCount.incrementAndGet();
    }

    // region Accessors

    @JsonProperty
    public UUID activityId() {
        return this.activityId;
    }

    @JsonSerialize(using = ToStringSerializer.class)
    @JsonProperty
    public Duration lifetime() {
        return this.lifetime.elapsed();
    }

    @JsonIgnore
    public long nanoTimeCreated() {
        return this.nanoTimeCreated;
    }

    @JsonProperty
    public String origin() {
        return this.origin;
    }

    @JsonIgnore
    public URI physicalAddress() {
        return this.physicalAddress;
    }

    @JsonProperty
    public String replicaPath() {
        return this.replicaPath;
    }

    @JsonIgnore
    public RxDocumentServiceRequest serviceRequest() {
        return this.serviceRequest;
    }

    @JsonProperty
    public OffsetDateTime timeCreated() {
        return this.timeCreated;
    }

    @JsonProperty
    public long transportRequestId() {
        return this.transportRequestId;
    }

    // endregion

    // region Methods

    public long stop(Timer requests, Timer responses) {
        this.lifetime.stop();
        this.sample.stop(requests);
        return this.sample.stop(responses);
    }

    @Override
    public String toString() {
        return RntbdObjectMapper.toString(this);
    }

    public void traceOperation(final Logger logger, final ChannelHandlerContext context, final String operationName, final Object... args) {

        checkNotNull(logger, "logger");

        if (logger.isTraceEnabled()) {
            final BigDecimal lifetime = BigDecimal.valueOf(this.lifetime.elapsed().toNanos(), 6);
            logger.trace("{},{},\"{}({})\",\"{}\",\"{}\"", this.timeCreated, lifetime, operationName,
                Stream.of(args).map(arg ->
                    arg == null ? "null" : arg.toString()).collect(Collectors.joining(",")
                ),
                this, context
            );
        }
    }

    // endregion
}
