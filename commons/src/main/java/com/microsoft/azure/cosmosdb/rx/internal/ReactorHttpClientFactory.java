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

package com.microsoft.azure.cosmosdb.rx.internal;

import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.proxy.HttpProxyHandler;
import io.netty.handler.ssl.SslHandler;
import org.slf4j.LoggerFactory;
import reactor.netty.NettyPipeline;
import reactor.netty.ReactorNetty;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

import java.net.InetSocketAddress;

/**
 * Helper class internally used for instantiating reactor netty http client.
 */
public class ReactorHttpClientFactory {
    private final static String REACTOR_NETWORK_LOG_CATEGORY = "com.microsoft.azure.cosmosdb.reactor-netty-network";

    private final Configs configs;
    private Integer maxPoolSize;
    private Integer maxIdleConnectionTimeoutInMillis;
    private Integer requestTimeoutInMillis;
    private InetSocketAddress proxy;

    public ReactorHttpClientFactory(Configs configs) {
        this.configs = configs;
    }

    public ReactorHttpClientFactory withPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
        return this;
    }

    public ReactorHttpClientFactory withHttpProxy(InetSocketAddress proxy) {
        this.proxy = proxy;
        return this;
    }

    public ReactorHttpClientFactory withMaxIdleConnectionTimeoutInMillis(int maxIdleConnectionTimeoutInMillis) {
        this.maxIdleConnectionTimeoutInMillis = maxIdleConnectionTimeoutInMillis;
        return this;
    }

    public ReactorHttpClientFactory withRequestTimeoutInMillis(int requestTimeoutInMillis) {
        this.requestTimeoutInMillis = requestTimeoutInMillis;
        return this;
    }

    public HttpClient toHttpClient() {
        if (configs == null) {
            throw new IllegalArgumentException("configs is null");
        }
        if (this.maxPoolSize == null) {
            maxPoolSize = Integer.parseInt(System.getProperty(ReactorNetty.POOL_MAX_CONNECTIONS,
                    "" + Math.max(Runtime.getRuntime()
                            .availableProcessors(), 8) * 2));
        }
        //  TODO: maxIdleConnectionTimeoutInMillis is not supported in reactor netty

        HttpClient httpClient = HttpClient.create(ConnectionProvider.fixed("reactor-netty-connection-pool", maxPoolSize));

        //  Configure the client with channel pipeline
        httpClient = configureChannelPipelineHandlers(httpClient);

        return httpClient;
    }

    private synchronized HttpClient configureChannelPipelineHandlers(HttpClient httpClient) {
        return httpClient.tcpConfiguration(tcpClient -> tcpClient.doOnConnected(connection -> {
            ChannelPipeline pipeline = connection.channel().pipeline();
            if (LoggerFactory.getLogger(REACTOR_NETWORK_LOG_CATEGORY).isTraceEnabled()
                    && pipeline.get(NettyPipeline.LoggingHandler) == null) {
                pipeline.addFirst(NettyPipeline.LoggingHandler,
                        new LoggingHandler(REACTOR_NETWORK_LOG_CATEGORY, LogLevel.TRACE));
            }
            if (proxy != null
                    && pipeline.get(NettyPipeline.ProxyHandler) == null) {
                pipeline.addFirst(NettyPipeline.ProxyHandler,
                        new HttpProxyHandler(proxy));
            }
            if (pipeline.get(NettyPipeline.SslHandler) == null) {
                pipeline.addFirst(NettyPipeline.SslHandler,
                        new SslHandler(this.configs.getSslContext().newEngine(pipeline.channel().alloc())));
            }

            if (pipeline.get(NettyPipeline.HttpCodec) == null) {
                pipeline.addFirst(NettyPipeline.HttpCodec,
                        new HttpClientCodec(this.configs.getMaxHttpInitialLineLength(),
                                this.configs.getMaxHttpHeaderSize(),
                                this.configs.getMaxHttpChunkSize(),
                                true,
                                true));
            }

            if (pipeline.get(NettyPipeline.HttpAggregator) == null) {
                pipeline.addFirst(NettyPipeline.HttpAggregator,
                        new HttpObjectAggregator(this.configs.getMaxHttpBodyLength()));
            }
        }));
    }
}
