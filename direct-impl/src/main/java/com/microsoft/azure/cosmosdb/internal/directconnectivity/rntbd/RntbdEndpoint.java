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
import com.google.common.base.Strings;
import com.microsoft.azure.cosmosdb.internal.UserAgentContainer;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.ssl.SslContext;

import java.io.IOException;
import java.net.URI;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.microsoft.azure.cosmosdb.internal.directconnectivity.RntbdTransportClient.Options;

public interface RntbdEndpoint extends AutoCloseable {

    String name();

    @Override
    void close() throws RuntimeException;

    RntbdRequestRecord request(RntbdRequestArgs requestArgs);

    interface Provider extends AutoCloseable {

        @Override
        void close() throws RuntimeException;

        Config config();

        int count();

        RntbdEndpoint get(URI physicalAddress);

        Stream<RntbdEndpoint> list();
    }

    @JsonSerialize(using = Config.JsonSerializer.class)
    final class Config {

        private final Options options;
        private final SslContext sslContext;
        private final LogLevel wireLogLevel;

        public Config(final Options options, final SslContext sslContext, final LogLevel wireLogLevel) {

            checkNotNull(options, "options");
            checkNotNull(sslContext, "sslContext");

            this.options = options;
            this.sslContext = sslContext;
            this.wireLogLevel = wireLogLevel;
        }

        public int connectionTimeout() {
            final long value = this.options.connectionTimeout().toMillis();
            assert value <= Integer.MAX_VALUE;
            return (int)value;
        }

        public long idleConnectionTimeout() {
            return this.options.idleTimeout().toNanos();
        }

        public int maxChannelsPerEndpoint() {
            return this.options.maxChannelsPerEndpoint();
        }

        public int maxRequestsPerChannel() {
            return this.options.maxRequestsPerChannel();
        }

        public long receiveHangDetectionTime() {
            return this.options.receiveHangDetectionTime().toNanos();
        }

        public long requestTimeout() {
            return this.options.requestTimeout().toNanos();
        }

        public long sendHangDetectionTime() {
            return this.options.sendHangDetectionTime().toNanos();
        }

        public long shutdownTimeout() {
            return this.options.shutdownTimeout().toNanos();
        }

        public SslContext sslContext() {
            return this.sslContext;
        }

        public UserAgentContainer userAgent() {
            return this.options.userAgent();
        }

        public LogLevel wireLogLevel() {
            return this.wireLogLevel;
        }

        @Override
        public String toString() {
            return "RntbdEndpoint.Config(" + RntbdObjectMapper.toJson(this) + ')';
        }

        static class JsonSerializer extends StdSerializer<Config> {

            public JsonSerializer() {
                super(Config.class);
            }

            @Override
            public void serialize(Config value, JsonGenerator generator, SerializerProvider provider) throws IOException {
                generator.writeStartObject();
                generator.writeNumberField("connectionTimeout", value.connectionTimeout());
                generator.writeNumberField("idleConnectionTimeout", value.idleConnectionTimeout());
                generator.writeNumberField("maxChannelPerEndpoint", value.maxChannelsPerEndpoint());
                generator.writeNumberField("maxRequestsPerChannel", value.maxChannelsPerEndpoint());
                generator.writeNumberField("receiveHangDetectionTime", value.receiveHangDetectionTime());
                generator.writeNumberField("requestTimeout", value.requestTimeout());
                generator.writeNumberField("sendHangDetectionTime", value.sendHangDetectionTime());
                generator.writeNumberField("shutdownTimeout", value.shutdownTimeout());
                generator.writeObjectField("userAgent", value.userAgent());
                generator.writeStringField("wireLogLevel", value.wireLogLevel() == null ? null : value.wireLogLevel().toString());
                generator.writeEndObject();
            }
        }
    }
}
