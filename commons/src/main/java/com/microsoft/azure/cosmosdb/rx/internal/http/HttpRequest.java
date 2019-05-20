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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpMethod;
import reactor.core.publisher.Flux;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

/**
 * The outgoing Http request.
 */
public class HttpRequest {
    private HttpMethod httpMethod;
    private URL url;
    private int port;
    private HttpHeaders headers;
    private Flux<ByteBuf> body;

    /**
     * Create a new HttpRequest instance.
     *
     * @param httpMethod the HTTP request method
     * @param url the target address to send the request to
     */
    public HttpRequest(HttpMethod httpMethod, URL url, int port) {
        this.httpMethod = httpMethod;
        this.url = url;
        this.port = port;
        this.headers = new HttpHeaders();
    }

    /**
     * Create a new HttpRequest instance.
     *
     * @param httpMethod the HTTP request method
     * @param url the target address to send the request to
     */
    public HttpRequest(HttpMethod httpMethod, String url, int port) throws MalformedURLException {
        this.httpMethod = httpMethod;
        this.url = new URL(url);
        this.port = port;
        this.headers = new HttpHeaders();
    }

    /**
     * Create a new HttpRequest instance.
     *
     * @param httpMethod the HTTP request method
     * @param url the target address to send the request to
     * @param headers the HTTP headers to use with this request
     * @param body the request content
     */
    public HttpRequest(HttpMethod httpMethod, URL url, int port, HttpHeaders headers, Flux<ByteBuf> body) {
        this.httpMethod = httpMethod;
        this.url = url;
        this.port = port;
        this.headers = headers;
        this.body = body;
    }

    /**
     * Get the request method.
     *
     * @return the request method
     */
    public HttpMethod httpMethod() {
        return httpMethod;
    }

    /**
     * Set the request method.
     *
     * @param httpMethod the request method
     * @return this HttpRequest
     */
    public HttpRequest withHttpMethod(HttpMethod httpMethod) {
        this.httpMethod = httpMethod;
        return this;
    }

    /**
     * Get the target port.
     *
     * @return the target port
     */
    public int port() {
        return port;
    }

    /**
     * Set the target port to send the request to.
     *
     * @param port target port
     * @return this HttpRequest
     */
    public HttpRequest withPort(int port) {
        this.port = port;
        return this;
    }

    /**
     * Get the target address.
     *
     * @return the target address
     */
    public URL url() {
        return url;
    }

    /**
     * Set the target address to send the request to.
     *
     * @param url target address as {@link URL}
     * @return this HttpRequest
     */
    public HttpRequest withUrl(URL url) {
        this.url = url;
        return this;
    }

    /**
     * Get the request headers.
     *
     * @return headers to be sent
     */
    public HttpHeaders headers() {
        return headers;
    }

    /**
     * Set the request headers.
     *
     * @param headers the set of headers
     * @return this HttpRequest
     */
    public HttpRequest withHeaders(HttpHeaders headers) {
        this.headers = headers;
        return this;
    }

    /**
     * Set a request header, replacing any existing value.
     * A null for {@code value} will remove the header if one with matching name exists.
     *
     * @param name the header name
     * @param value the header value
     * @return this HttpRequest
     */
    public HttpRequest withHeader(String name, String value) {
        headers.set(name, value);
        return this;
    }

    /**
     * Get the request content.
     *
     * @return the content to be send
     */
    public Flux<ByteBuf> body() {
        return body;
    }

    /**
     * Set the request content.
     *
     * @param content the request content
     * @return this HttpRequest
     */
    public HttpRequest withBody(String content) {
        final byte[] bodyBytes = content.getBytes(StandardCharsets.UTF_8);
        return withBody(bodyBytes);
    }

    /**
     * Set the request content.
     * The Content-Length header will be set based on the given content's length
     *
     * @param content the request content
     * @return this HttpRequest
     */
    public HttpRequest withBody(byte[] content) {
        headers.set("Content-Length", String.valueOf(content.length));
        // Unpooled.wrappedBuffer(body) allocates ByteBuf from unpooled heap
        return withBody(Flux.defer(() -> Flux.just(Unpooled.wrappedBuffer(content))));
    }

    /**
     * Set request content.
     *
     * Caller must set the Content-Length header to indicate the length of the content,
     * or use Transfer-Encoding: chunked.
     *
     * @param content the request content
     * @return this HttpRequest
     */
    public HttpRequest withBody(Flux<ByteBuf> content) {
        this.body = content;
        return this;
    }

    /**
     * Creates a clone of the request.
     *
     * The main purpose of this is so that this HttpRequest can be changed and the resulting
     * HttpRequest can be a backup. This means that the buffered HttpHeaders and body must
     * not be able to change from side effects of this HttpRequest.
     *
     * @return a new HTTP request instance with cloned instances of all mutable properties.
     */
    public HttpRequest buffer() {
        final HttpHeaders bufferedHeaders = new HttpHeaders(headers);
        return new HttpRequest(httpMethod, url, port, bufferedHeaders, body);
    }
}
