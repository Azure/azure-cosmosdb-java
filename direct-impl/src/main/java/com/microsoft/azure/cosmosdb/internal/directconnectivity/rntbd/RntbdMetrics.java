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
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;

import java.time.Duration;
import java.time.OffsetDateTime;

@JsonPropertyOrder({ "creationTime", "lifetime" })
public class RntbdMetrics {

    private static final CompositeMeterRegistry registry = new CompositeMeterRegistry();
    private final OffsetDateTime creationTime;
    private final Stopwatch lifetime;
    private final String prefix;

    RntbdMetrics(final Class<?> cls, final long instanceId) {
        this.creationTime = OffsetDateTime.now();
        this.lifetime = Stopwatch.createStarted();
        this.prefix = Strings.lenientFormat("%s[%s].", cls.getName(), instanceId);
    }

    @JsonSerialize(using = ToStringSerializer.class)
    @JsonProperty
    public final OffsetDateTime creationTime() {
        return this.creationTime;
    }

    @JsonSerialize(using = ToStringSerializer.class)
    @JsonProperty
    public final Duration lifetime() {
        return this.lifetime.elapsed();
    }

    final String nameOf(final String member) {
        return prefix + member;
    }

    @Override
    public final String toString() {
        return RntbdObjectMapper.toString(this);
    }

    static MeterRegistry registry() {
        return RntbdMetrics.registry;
    }
}
