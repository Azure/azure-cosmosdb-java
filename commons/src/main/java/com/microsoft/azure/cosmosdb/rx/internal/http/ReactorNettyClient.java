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
 */
package com.microsoft.azure.cosmosdb.rx.internal.http;

import com.microsoft.azure.cosmosdb.rx.internal.Configs;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponseDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.ssl.SslHandler;
import org.reactivestreams.Publisher;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.ByteBufFlux;
import reactor.netty.Connection;
import reactor.netty.NettyOutbound;
import reactor.netty.NettyPipeline;
import reactor.netty.channel.BootstrapHandlers;
import reactor.netty.http.client.HttpClientRequest;
import reactor.netty.http.client.HttpClientResponse;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.tcp.ProxyProvider;
import reactor.netty.tcp.SslProvider;
import reactor.netty.tcp.TcpResources;

import java.nio.charset.Charset;
import java.util.Objects;
import java.util.function.BiFunction;

import static com.microsoft.azure.cosmosdb.rx.internal.http.HttpClientConfig.REACTOR_NETWORK_LOG_CATEGORY;

/**
 * HttpClient that is implemented using reactor-netty.
 */
class ReactorNettyClient implements HttpClient {

    private HttpClientConfig httpClientConfig;
    private reactor.netty.http.client.HttpClient httpClient;
    private ConnectionProvider connectionProvider;

    /**
     * Creates ReactorNettyClient with {@link ConnectionProvider}.
     */
    ReactorNettyClient(ConnectionProvider connectionProvider, HttpClientConfig httpClientConfig) {
        this.connectionProvider = connectionProvider;
        this.httpClientConfig = httpClientConfig;
        this.httpClient = configureChannelPipelineHandlers(reactor.netty.http.client.HttpClient.create(connectionProvider));
    }

    private reactor.netty.http.client.HttpClient configureChannelPipelineHandlers(reactor.netty.http.client.HttpClient httpClient) {
        Configs configs = this.httpClientConfig.getConfigs();
        if (LoggerFactory.getLogger(REACTOR_NETWORK_LOG_CATEGORY).isTraceEnabled()) {
            httpClient = httpClient.tcpConfiguration(tcpClient -> tcpClient.wiretap(REACTOR_NETWORK_LOG_CATEGORY, LogLevel.TRACE));
        }
        if (this.httpClientConfig.getProxy() != null) {
            httpClient = httpClient.tcpConfiguration(tcpClient ->
                    tcpClient.proxy(typeSpec -> typeSpec.type(ProxyProvider.Proxy.HTTP).address(this.httpClientConfig.getProxy())));
        }

        httpClient = httpClient.tcpConfiguration(tcpClient -> {
            tcpClient = tcpClient.secure(SslProvider.defaultClientProvider());
            Objects.requireNonNull(tcpClient.sslProvider())
                    .configure(new SslHandler(configs.getSslContext().newEngine(ByteBufAllocator.DEFAULT)));
            return tcpClient;
        });

        return httpClient.tcpConfiguration(client -> client.bootstrap(bootstrap -> {
            BootstrapHandlers.updateConfiguration(bootstrap,
                    NettyPipeline.HttpCodec,
                    (connectionObserver, channel) ->
                            channel.pipeline().addLast(new HttpResponseDecoder(configs.getMaxHttpInitialLineLength(),
                                    configs.getMaxHttpHeaderSize(),
                                    configs.getMaxHttpChunkSize(),
                                    true)));

            //  NOTE: Pooled connection time-out is not supported in reactor-netty
            //  https://github.com/reactor/reactor-netty/issues/612

//            Integer maxIdleConnectionTimeoutInMillis;
//
//            if (this.httpClientConfig.getMaxIdleConnectionTimeoutInMillis() != null) {
//                maxIdleConnectionTimeoutInMillis = this.httpClientConfig.getMaxIdleConnectionTimeoutInMillis();
//            } else {
//                maxIdleConnectionTimeoutInMillis = MAX_IDLE_CONNECTION_TIMEOUT_IN_MILLIS;
//            }
//
//            BootstrapHandlers.updateConfiguration(bootstrap,
//                    "idleStateHandler",
//                    ((connectionObserver, channel) ->
//                            channel.pipeline().addLast(
//                                    new IdleStateHandler(0, 0, maxIdleConnectionTimeoutInMillis))));

            BootstrapHandlers.updateConfiguration(bootstrap,
                    NettyPipeline.HttpAggregator,
                    ((connectionObserver, channel) ->
                            channel.pipeline().addLast(
                                    new HttpObjectAggregator(configs.getMaxHttpBodyLength()))));
            return bootstrap;
        }));
    }

    @Override
    public Mono<HttpResponse> send(final HttpRequest request) {
        Objects.requireNonNull(request.httpMethod());
        Objects.requireNonNull(request.url());
        Objects.requireNonNull(request.url().getProtocol());
        Objects.requireNonNull(this.httpClientConfig);

        return httpClient
                .tcpConfiguration(tcpClient -> tcpClient.port(request.port()))
                .request(HttpMethod.valueOf(request.httpMethod().toString()))
                .uri(request.url().toString())
                .send(bodySendDelegate(request))
                .responseConnection(responseDelegate(request))
                .single();
    }

    /**
     * Delegate to send the request content.
     *
     * @param restRequest the Rest request contains the body to be sent
     * @return a delegate upon invocation sets the request body in reactor-netty outbound object
     */
    private static BiFunction<HttpClientRequest, NettyOutbound, Publisher<Void>> bodySendDelegate(final HttpRequest restRequest) {
        return (reactorNettyRequest, reactorNettyOutbound) -> {
            for (HttpHeader header : restRequest.headers()) {
                reactorNettyRequest.header(header.name(), header.value());
            }
            if (restRequest.body() != null) {
                Flux<ByteBuf> nettyByteBufFlux = restRequest.body().map(Unpooled::wrappedBuffer);
                return reactorNettyOutbound.options(sendOptions -> sendOptions.flushOnEach(false)).send(nettyByteBufFlux);
            } else {
                return reactorNettyOutbound.options(sendOptions -> sendOptions.flushOnEach(false));
            }
        };
    }

    /**
     * Delegate to receive response.
     *
     * @param restRequest the Rest request whose response this delegate handles
     * @return a delegate upon invocation setup Rest response object
     */
    private static BiFunction<HttpClientResponse, Connection, Publisher<HttpResponse>> responseDelegate(final HttpRequest restRequest) {
        return (reactorNettyResponse, reactorNettyConnection) ->
                Mono.just(new ReactorNettyHttpResponse(reactorNettyResponse, reactorNettyConnection).withRequest(restRequest));
    }

    @Override
    public void shutdown() {
        TcpResources.disposeLoopsAndConnections();
        this.connectionProvider.dispose();
    }

    private static class ReactorNettyHttpResponse extends HttpResponse {
        private final HttpClientResponse reactorNettyResponse;
        private final Connection reactorNettyConnection;

        ReactorNettyHttpResponse(HttpClientResponse reactorNettyResponse, Connection reactorNettyConnection) {
            this.reactorNettyResponse = reactorNettyResponse;
            this.reactorNettyConnection = reactorNettyConnection;
        }

        @Override
        public int statusCode() {
            return reactorNettyResponse.status().code();
        }

        @Override
        public String headerValue(String name) {
            return reactorNettyResponse.responseHeaders().get(name);
        }

        @Override
        public HttpHeaders headers() {
            HttpHeaders headers = new HttpHeaders();
            reactorNettyResponse.responseHeaders().forEach(e -> headers.set(e.getKey(), e.getValue()));
            return headers;
        }

        @Override
        public Flux<ByteBuf> body() {
            return bodyIntern().doFinally(s -> {
                reactorNettyConnection.dispose();
                if (!reactorNettyConnection.isDisposed()) {
                    reactorNettyConnection.channel().eventLoop().execute(reactorNettyConnection::dispose);
                }
            });
        }

        @Override
        public Mono<byte[]> bodyAsByteArray() {
            return bodyIntern().aggregate().asByteArray().doFinally(s -> {
                reactorNettyConnection.dispose();
                if (!reactorNettyConnection.isDisposed()) {
                    reactorNettyConnection.channel().eventLoop().execute(reactorNettyConnection::dispose);
                }
            });
        }

        @Override
        public Mono<String> bodyAsString() {
            return bodyIntern().aggregate().asString().doFinally(s -> {
                reactorNettyConnection.dispose();
                if (!reactorNettyConnection.isDisposed()) {
                    reactorNettyConnection.channel().eventLoop().execute(reactorNettyConnection::dispose);
                }
            });
        }

        @Override
        public Mono<String> bodyAsString(Charset charset) {
            return bodyIntern().aggregate().asString(charset).doFinally(s -> {
                reactorNettyConnection.dispose();
                if (!reactorNettyConnection.isDisposed()) {
                    reactorNettyConnection.channel().eventLoop().execute(reactorNettyConnection::dispose);
                }
            });
        }

        @Override
        public void close() {
            reactorNettyConnection.dispose();
            if (!reactorNettyConnection.isDisposed()) {
                reactorNettyConnection.channel().eventLoop().execute(reactorNettyConnection::dispose);
            }
        }

        private ByteBufFlux bodyIntern() {
            return reactorNettyConnection.inbound().receive();
        }

        @Override
        Connection internConnection() {
            return reactorNettyConnection;
        }
    }
}
