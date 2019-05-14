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

import java.util.Objects;
import java.util.Optional;

/**
 * Type representing context local to a single http request and it's response.
 */
public final class HttpPipelineCallContext {
    private HttpRequest httpRequest;
    private ContextData data;

    //<editor-fold defaultstate="collapsed" desc="Package internal methods">
    /**
     * Package private ctr.
     *
     * Creates HttpPipelineCallContext.
     *
     * @param httpRequest the request for which context needs to be created
     *
     * @throws IllegalArgumentException if there are multiple policies with same name
     */
    HttpPipelineCallContext(HttpRequest httpRequest) {
        this(httpRequest, ContextData.NONE);
    }

    /**
     * Package private ctr.
     *
     * Creates HttpPipelineCallContext.
     *
     * @param httpRequest the request for which context needs to be created
     * @param data the data to associate with this context
     *
     * @throws IllegalArgumentException if there are multiple policies with same name
     */
    HttpPipelineCallContext(HttpRequest httpRequest, ContextData data) {
        Objects.requireNonNull(httpRequest);
        Objects.requireNonNull(data);
        //
        this.httpRequest = httpRequest;
        this.data = data;
    }
    //</editor-fold>

    //<editor-fold defaultstate="collapsed" desc="Public methods">

    /**
     * Stores a key-value data in the context.
     *
     * @param key the key
     * @param value the value
     */
    public void setData(String key, Object value) {
        this.data = this.data.addData(key, value);
    }

    /**
     * Gets a value with the given key stored in the context.
     *
     * @param key the key
     * @return the value
     */
    public Optional<Object> getData(String key) {
        return this.data.getData(key);
    }

    /**
     * Get the http request.
     *
     * @return the request.
     */
    public HttpRequest httpRequest() {
        return this.httpRequest;
    }

    /**
     * Sets the http request object in the context.
     *
     * @param request request object
     * @return HttpPipelineCallContext
     */
    public HttpPipelineCallContext withHttpRequest(HttpRequest request) {
        this.httpRequest = request;
        return this;
    }

    //</editor-fold>
}
