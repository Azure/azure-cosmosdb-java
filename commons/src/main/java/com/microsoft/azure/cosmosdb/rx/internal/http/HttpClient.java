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
import reactor.netty.ReactorNetty;
import reactor.netty.resources.ConnectionProvider;

import java.util.function.Supplier;

/**
 * A generic interface for sending HTTP requests and getting responses.
 */
public interface HttpClient {

    String REACTOR_NETTY_CONNECTION_POOL = "reactor-netty-connection-pool";

    /**
     * Send the provided request asynchronously.
     *
     * @param request The HTTP request to send
     * @return A {@link Mono} that emits response asynchronously
     */
    Mono<HttpResponse> send(HttpRequest request);

    /**
     * Create default HttpClient instance.
     *
     * @return the HttpClient
     */
    static HttpClient createDefault() {
        return new ReactorNettyClient();
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

        Integer maxPoolSize = httpClientConfig.getMaxPoolSize();
        if (maxPoolSize == null) {
            maxPoolSize = Integer.parseInt(System.getProperty(ReactorNetty.POOL_MAX_CONNECTIONS,
                    "" + Math.max(Runtime.getRuntime()
                            .availableProcessors(), 8) * 2));
        }
        //  TODO: maxIdleConnectionTimeoutInMillis is not supported in reactor netty

        ConnectionProvider fixed = ConnectionProvider.fixed(REACTOR_NETTY_CONNECTION_POOL, maxPoolSize);
        return new ReactorNettyClient(fixed, httpClientConfig);
    }

    /**
     * Apply the provided proxy configuration to the HttpClient.
     *
     * @param proxyOptions the proxy configuration supplier
     * @return a HttpClient with proxy applied
     */
    HttpClient proxy(Supplier<ProxyOptions> proxyOptions);

    /**
     * Apply or remove a wire logger configuration.
     *
     * @param enableWiretap wiretap config
     * @return a HttpClient with wire logging enabled or disabled
     */
    HttpClient wiretap(boolean enableWiretap);

    /**
     * Set the port that client should connect to.
     *
     * @param port the port
     * @return a HttpClient with port applied
     */
    HttpClient port(int port);

    /**
     * Shutdown the Http Client and clean up resources
     */
    void shutdown();
}
