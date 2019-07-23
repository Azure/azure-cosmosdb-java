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
import com.microsoft.azure.cosmosdb.internal.UserAgentContainer;
import io.micrometer.core.instrument.Tag;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.ssl.SslContext;

import java.net.SocketAddress;
import java.net.URI;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.microsoft.azure.cosmosdb.internal.directconnectivity.RntbdTransportClient.Options;

public interface RntbdEndpoint extends AutoCloseable {

    // region Accessors

    int channelsAcquired();

    int channelsAvailable();

    int concurrentRequests();

    long id();

    boolean isClosed();

    SocketAddress remoteAddress();

    int requestQueueLength();

    Tag tag();

    long usedDirectMemory();

    long usedHeapMemory();

    // endregion

    // region Methods

    @Override
    void close();

    RntbdRequestRecord request(RntbdRequestArgs requestArgs);

    // endregion

    // region Types

    interface Provider extends AutoCloseable {

        @Override
        void close();

        Config config();

        int count();

        RntbdEndpoint get(URI physicalAddress);

        Stream<RntbdEndpoint> list();
    }

    final class Config {

        private final PooledByteBufAllocator allocator;
        private final Options options;
        private final SslContext sslContext;
        private final LogLevel wireLogLevel;

        public Config(final Options options, final SslContext sslContext, final LogLevel wireLogLevel) {

            checkNotNull(options, "options");
            checkNotNull(sslContext, "sslContext");

            int directArenaCount = PooledByteBufAllocator.defaultNumDirectArena();
            int heapArenaCount = PooledByteBufAllocator.defaultNumHeapArena();
            int pageSize = options.bufferPageSize();
            int maxOrder = Integer.numberOfTrailingZeros(options.maxBufferCapacity()) - Integer.numberOfTrailingZeros(pageSize);

            this.allocator = new PooledByteBufAllocator(heapArenaCount, directArenaCount, pageSize, maxOrder);
            this.options = options;
            this.sslContext = sslContext;
            this.wireLogLevel = wireLogLevel;
        }

        @JsonIgnore
        public PooledByteBufAllocator allocator() {
            return this.allocator;
        }

        @JsonProperty
        public int bufferPageSize() {
            return this.options.bufferPageSize();
        }

        @JsonProperty
        public int connectionTimeout() {
            final long value = this.options.connectionTimeout().toMillis();
            assert value <= Integer.MAX_VALUE;
            return (int)value;
        }

        @JsonProperty
        public long idleConnectionTimeout() {
            return this.options.idleTimeout().toNanos();
        }

        @JsonProperty
        public int maxBufferCapacity() {
            return this.options.maxBufferCapacity();
        }

        @JsonProperty
        public int maxChannelsPerEndpoint() {
            return this.options.maxChannelsPerEndpoint();
        }

        @JsonProperty
        public int maxRequestsPerChannel() {
            return this.options.maxRequestsPerChannel();
        }

        @JsonProperty
        public long receiveHangDetectionTime() {
            return this.options.receiveHangDetectionTime().toNanos();
        }

        @JsonProperty
        public long requestTimeout() {
            return this.options.requestTimeout().toNanos();
        }

        @JsonProperty
        public long sendHangDetectionTime() {
            return this.options.sendHangDetectionTime().toNanos();
        }

        @JsonProperty
        public long shutdownTimeout() {
            return this.options.shutdownTimeout().toNanos();
        }

        @JsonIgnore
        public SslContext sslContext() {
            return this.sslContext;
        }

        @JsonProperty
        public UserAgentContainer userAgent() {
            return this.options.userAgent();
        }

        @JsonProperty
        public LogLevel wireLogLevel() {
            return this.wireLogLevel;
        }

        @Override
        public String toString() {
            return RntbdObjectMapper.toString(this);
        }
    }

    // endregion
}
