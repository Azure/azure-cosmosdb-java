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

import reactor.core.publisher.Mono;
import reactor.netty.resources.ConnectionProvider;

/**
 * A generic interface for sending HTTP requests and getting responses.
 */
public interface HttpClient {

    String REACTOR_NETTY_CONNECTION_POOL = "reactor-netty-connection-pool";
    Integer MAX_IDLE_CONNECTION_TIMEOUT_IN_MILLIS = 60 * 1000;
    Integer REQUEST_TIMEOUT_IN_MILLIS = 60 * 1000;

    /**
     * Send the provided request asynchronously.
     *
     * @param request The HTTP request to send
     * @return A {@link Mono} that emits response asynchronously
     */
    Mono<HttpResponse> send(HttpRequest request);

    /**
     * Create elastic HttpClient with {@link HttpClientConfig}
     *
     * @return the HttpClient
     */
    static HttpClient createElastic(HttpClientConfig httpClientConfig) {
        return new ReactorNettyClient(ConnectionProvider.elastic(REACTOR_NETTY_CONNECTION_POOL), httpClientConfig);
    }

    /**
     * Create fixed HttpClient with {@link HttpClientConfig}
     *
     * @return the HttpClient
     */
    static HttpClient createFixed(HttpClientConfig httpClientConfig) {
        if (httpClientConfig.getConfigs() == null) {
            throw new IllegalArgumentException("HttpClientConfig is null");
        }

        if (httpClientConfig.getMaxPoolSize() == null) {
            return new ReactorNettyClient(ConnectionProvider.fixed(REACTOR_NETTY_CONNECTION_POOL), httpClientConfig);
        }
        return new ReactorNettyClient(ConnectionProvider.fixed(REACTOR_NETTY_CONNECTION_POOL, httpClientConfig.getMaxPoolSize()), httpClientConfig);
    }

    /**
     * Shutdown the Http Client and clean up resources
     */
    void shutdown();
}
