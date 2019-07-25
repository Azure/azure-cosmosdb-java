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

package com.microsoft.azure.cosmosdb.internal.directconnectivity;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.base.Strings;
import com.microsoft.azure.cosmosdb.DocumentClientException;
import com.microsoft.azure.cosmosdb.internal.UserAgentContainer;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.rntbd.RntbdEndpoint;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.rntbd.RntbdObjectMapper;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.rntbd.RntbdRequestArgs;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.rntbd.RntbdRequestRecord;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.rntbd.RntbdServiceEndpoint;
import com.microsoft.azure.cosmosdb.rx.internal.Configs;
import com.microsoft.azure.cosmosdb.rx.internal.RxDocumentServiceRequest;
import io.micrometer.core.instrument.Tag;
import io.netty.handler.ssl.SslContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Single;
import rx.SingleEmitter;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.microsoft.azure.cosmosdb.internal.directconnectivity.rntbd.RntbdReporter.reportIssueUnless;

@JsonSerialize(using = RntbdTransportClient.JsonSerializer.class)
public final class RntbdTransportClient extends TransportClient implements AutoCloseable {

    // region Fields

    private static final AtomicLong instanceCount = new AtomicLong();
    private static final Logger logger = LoggerFactory.getLogger(RntbdTransportClient.class);

    private final AtomicBoolean closed = new AtomicBoolean();
    private final RntbdEndpoint.Provider endpointProvider;
    private final long id;
    private final Tag tag;

    // endregion

    // region Constructors

    RntbdTransportClient(final RntbdEndpoint.Provider endpointProvider) {

        this.endpointProvider = endpointProvider;
        this.id = instanceCount.incrementAndGet();

        this.tag = Tag.of(RntbdTransportClient.class.getSimpleName(), Strings.padStart(
            Long.toHexString(this.id).toUpperCase(), 4, '0')
        );
    }

    RntbdTransportClient(final Options options, final SslContext sslContext) {

        this.endpointProvider = new RntbdServiceEndpoint.Provider(this, options, sslContext);
        this.id = instanceCount.incrementAndGet();

        this.tag = Tag.of(RntbdTransportClient.class.getSimpleName(), Strings.padStart(
            Long.toHexString(this.id).toUpperCase(), 4, '0')
        );
    }

    RntbdTransportClient(final Configs configs, final int requestTimeoutInSeconds, final UserAgentContainer userAgent) {
        this(new Options.Builder(requestTimeoutInSeconds).userAgent(userAgent).build(), configs.getSslContext());
    }

    // endregion

    // region Accessors

    public int endpointCount() {
        return this.endpointProvider.count();
    }

    public long id() {
        return this.id;
    }

    public boolean isClosed() {
        return this.closed.get();
    }

    public Tag tag() {
        return this.tag;
    }

    // endregion

    // region Methods

    @Override
    public void close() {

        logger.debug("\n  [{}] CLOSE", this);

        if (this.closed.compareAndSet(false, true)) {
            this.endpointProvider.close();
            return;
        }

        logger.debug("\n  [{}]\n  already closed", this);
    }

    @Override
    public Single<StoreResponse> invokeStoreAsync(
        final URI physicalAddress, final ResourceOperation unused, final RxDocumentServiceRequest request
    ) {
        checkNotNull(physicalAddress, "physicalAddress");
        checkNotNull(request, "request");
        this.throwIfClosed();

        final RntbdRequestArgs requestArgs = new RntbdRequestArgs(request, physicalAddress);
        requestArgs.traceOperation(logger, null, "invokeStoreAsync");

        final RntbdEndpoint endpoint = this.endpointProvider.get(physicalAddress);
        final RntbdRequestRecord record = endpoint.request(requestArgs);

        return Single.fromEmitter((SingleEmitter<StoreResponse> emitter) -> {

            record.whenComplete((response, error) -> {

                requestArgs.traceOperation(logger, null, "emitSingle", response, error);

                if (error == null) {
                    emitter.onSuccess(response);
                } else {
                    reportIssueUnless(logger, error instanceof DocumentClientException, record, "", error);
                    emitter.onError(error);
                }

                requestArgs.traceOperation(logger, null, "emitSingleComplete");
            });
        });
    }

    @Override
    public String toString() {
        return RntbdObjectMapper.toString(this);
    }

    private void throwIfClosed() {
        checkState(!this.closed.get(), "%s is closed", this);
    }

    // endregion

    // region Types

    static final class JsonSerializer extends StdSerializer<RntbdTransportClient> {

        public JsonSerializer() {
            super(RntbdTransportClient.class);
        }

        @Override
        public void serialize(RntbdTransportClient value, JsonGenerator generator, SerializerProvider provider) throws IOException {

            generator.writeStartObject();
            generator.writeNumberField("id", value.id());
            generator.writeBooleanField("isClosed", value.isClosed());
            generator.writeObjectField("configuration", value.endpointProvider.config());
            generator.writeArrayFieldStart("serviceEndpoints");

            value.endpointProvider.list().forEach(endpoint -> {
                try {
                    generator.writeObject(endpoint);
                } catch (IOException error) {
                    logger.error("failed to serialize instance {} due to:", value.id(), error);
                }
            });

            generator.writeEndArray();
            generator.writeEndObject();
        }
    }

    public static final class Options {

        // region Fields

        private final int bufferPageSize;
        private final String certificateHostNameOverride;
        private final Duration connectionTimeout;
        private final Duration idleTimeout;
        private final int maxBufferCapacity;
        private final int maxChannelsPerEndpoint;
        private final int maxRequestsPerChannel;
        private final int partitionCount;
        private final Duration receiveHangDetectionTime;
        private final Duration requestTimeout;
        private final Duration sendHangDetectionTime;
        private final Duration shutdownTimeout;
        private final UserAgentContainer userAgent;

        // endregion

        // region Constructors

        private Options(Builder builder) {
            this.bufferPageSize = builder.bufferPageSize;
            this.certificateHostNameOverride = builder.certificateHostNameOverride;
            this.connectionTimeout = builder.connectionTimeout == null ? builder.requestTimeout : builder.connectionTimeout;
            this.idleTimeout = builder.idleTimeout;
            this.maxBufferCapacity = builder.maxBufferCapacity;
            this.maxChannelsPerEndpoint = builder.maxChannelsPerEndpoint;
            this.maxRequestsPerChannel = builder.maxRequestsPerChannel;
            this.partitionCount = builder.partitionCount;
            this.receiveHangDetectionTime = builder.receiveHangDetectionTime;
            this.requestTimeout = builder.requestTimeout;
            this.sendHangDetectionTime = builder.sendHangDetectionTime;
            this.shutdownTimeout = builder.shutdownTimeout;
            this.userAgent = builder.userAgent;
        }

        // endregion

        // region Accessors

        public int bufferPageSize() {
            return this.bufferPageSize;
        }

        public String certificateHostNameOverride() {
            return this.certificateHostNameOverride;
        }

        public Duration connectionTimeout() {
            return this.connectionTimeout;
        }

        public Duration idleTimeout() {
            return this.idleTimeout;
        }

        public int maxBufferCapacity() {
            return this.maxBufferCapacity;
        }

        public int maxChannelsPerEndpoint() {
            return this.maxChannelsPerEndpoint;
        }

        public int maxRequestsPerChannel() {
            return this.maxRequestsPerChannel;
        }

        public int partitionCount() {
            return this.partitionCount;
        }

        public Duration receiveHangDetectionTime() {
            return this.receiveHangDetectionTime;
        }

        public Duration requestTimeout() {
            return this.requestTimeout;
        }

        public Duration sendHangDetectionTime() {
            return this.sendHangDetectionTime;
        }

        public Duration shutdownTimeout() {
            return this.shutdownTimeout;
        }

        public UserAgentContainer userAgent() {
            return this.userAgent;
        }

        // endregion

        // region Methods

        @Override
        public String toString() {
            return RntbdObjectMapper.toJson(this);
        }

        // endregion

        // region Types

        public static class Builder {

            // region Fields

            private static final UserAgentContainer DEFAULT_USER_AGENT_CONTAINER = new UserAgentContainer();
            private static final Duration FIFTEEN_SECONDS = Duration.ofSeconds(15L);
            private static final Duration SIXTY_FIVE_SECONDS = Duration.ofSeconds(65L);
            private static final Duration TEN_SECONDS = Duration.ofSeconds(10L);

            private int bufferPageSize = 8192;
            private String certificateHostNameOverride = null;
            private Duration connectionTimeout = null;
            private Duration idleTimeout = Duration.ZERO;
            private int maxBufferCapacity = 8192 << 10;
            private int maxChannelsPerEndpoint = 10;
            private int maxRequestsPerChannel = 30;
            private int partitionCount = 1;
            private Duration receiveHangDetectionTime = SIXTY_FIVE_SECONDS;
            private Duration requestTimeout;
            private Duration sendHangDetectionTime = TEN_SECONDS;
            private Duration shutdownTimeout = FIFTEEN_SECONDS;
            private UserAgentContainer userAgent = DEFAULT_USER_AGENT_CONTAINER;

            // endregion

            // region Constructors

            public Builder(Duration requestTimeout) {
                this.requestTimeout(requestTimeout);
            }

            public Builder(int requestTimeoutInSeconds) {
                this(Duration.ofSeconds(requestTimeoutInSeconds));
            }

            // endregion

            // region Methods

            public Options build() {
                checkState(this.bufferPageSize <= this.maxBufferCapacity, "bufferPageSize (%s) > maxBufferCapacity (%s)",
                    this.bufferPageSize, this.maxBufferCapacity
                );
                return new Options(this);
            }

            public Builder bufferPageSize(final int value) {
                checkArgument(value >= 4096 && (value & (value - 1)) == 0, "value: %s", value);
                this.bufferPageSize = value;
                return this;
            }

            public Builder certificateHostNameOverride(final String value) {
                this.certificateHostNameOverride = value;
                return this;
            }

            public Builder connectionTimeout(final Duration value) {
                checkArgument(value == null || value.compareTo(Duration.ZERO) > 0, "value: %s", value);
                this.connectionTimeout = value;
                return this;
            }

            public Builder idleTimeout(final Duration value) {
                checkNotNull(value, "value: null");
                this.idleTimeout = value;
                return this;
            }

            public Builder maxBufferCapacity(final int value) {
                checkArgument(value > 0 && (value & (value - 1)) == 0, "value: %s", value);
                this.maxBufferCapacity = value;
                return this;
            }

            public Builder maxChannelsPerEndpoint(final int value) {
                checkArgument(value > 0, "value: %s", value);
                this.maxChannelsPerEndpoint = value;
                return this;
            }

            public Builder maxRequestsPerChannel(final int value) {
                checkArgument(value > 0, "value: %s", value);
                this.maxRequestsPerChannel = value;
                return this;
            }

            public Builder partitionCount(final int value) {
                checkArgument(value > 0, "value: %s", value);
                this.partitionCount = value;
                return this;
            }

            public Builder receiveHangDetectionTime(final Duration value) {

                checkNotNull(value, "value: null");
                checkArgument(value.compareTo(Duration.ZERO) > 0, "value: %s", value);

                this.receiveHangDetectionTime = value;
                return this;
            }

            public Builder requestTimeout(final Duration value) {

                checkNotNull(value, "value: null");
                checkArgument(value.compareTo(Duration.ZERO) > 0, "value: %s", value);

                this.requestTimeout = value;
                return this;
            }

            public Builder sendHangDetectionTime(final Duration value) {

                checkNotNull(value, "value: null");
                checkArgument(value.compareTo(Duration.ZERO) > 0, "value: %s", value);

                this.sendHangDetectionTime = value;
                return this;
            }

            public Builder shutdownTimeout(final Duration value) {

                checkNotNull(value, "value: null");
                checkArgument(value.compareTo(Duration.ZERO) > 0, "value: %s", value);

                this.shutdownTimeout = value;
                return this;
            }

            public Builder userAgent(final UserAgentContainer value) {
                checkNotNull(value, "value: null");
                this.userAgent = value;
                return this;
            }

            // endregion
        }

        // endregion
    }

    // endregion
}
