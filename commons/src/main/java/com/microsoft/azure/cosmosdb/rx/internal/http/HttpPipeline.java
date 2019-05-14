// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
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

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * The http pipeline.
 */
public final class HttpPipeline {
    private final HttpClient httpClient;
    private final HttpPipelinePolicy[] pipelinePolicies;

    /**
     * Creates a HttpPipeline holding array of policies that gets applied to all request initiated through
     * {@link HttpPipeline#send(HttpPipelineCallContext)} and it's response.
     *
     * @param httpClient the http client to write request to wire and receive response from wire.
     * @param pipelinePolicies pipeline policies in the order they need to applied, a copy of this array will
     *                                  be made hence changing the original array after the creation of pipeline
     *                                  will not  mutate the pipeline
     */
    public HttpPipeline(HttpClient httpClient, HttpPipelinePolicy... pipelinePolicies) {
        Objects.requireNonNull(httpClient);
        Objects.requireNonNull(pipelinePolicies);
        this.pipelinePolicies = Arrays.copyOf(pipelinePolicies, pipelinePolicies.length);
        this.httpClient = httpClient;
    }

    /**
     * Creates a HttpPipeline holding array of policies that gets applied all request initiated through
     * {@link HttpPipeline#send(HttpPipelineCallContext)} and it's response.
     *
     * The default HttpClient {@link HttpClient#createDefault()} will be used to write request to wire and
     * receive response from wire.
     *
     * @param pipelinePolicies pipeline policies in the order they need to applied, a copy of this array will
     *                                  be made hence changing the original array after the creation of pipeline
     *                                  will not  mutate the pipeline
     */
    public HttpPipeline(HttpPipelinePolicy... pipelinePolicies) {
        this(HttpClient.createDefault(), pipelinePolicies);
    }

    /**
     * Creates a HttpPipeline holding array of policies that gets applied to all request initiated through
     * {@link HttpPipeline#send(HttpPipelineCallContext)} and it's response.
     *
     * @param httpClient the http client to write request to wire and receive response from wire.
     * @param pipelinePolicies pipeline policies in the order they need to applied, a copy of this list
     *                         will be made so changing the original list after the creation of pipeline
     *                         will not mutate the pipeline
     */
    public HttpPipeline(HttpClient httpClient, List<HttpPipelinePolicy> pipelinePolicies) {
        Objects.requireNonNull(httpClient);
        Objects.requireNonNull(pipelinePolicies);
        this.pipelinePolicies = pipelinePolicies.toArray(new HttpPipelinePolicy[0]);
        this.httpClient = httpClient;
    }

    /**
     * Creates a HttpPipeline holding array of policies that gets applied all request initiated through
     * {@link HttpPipeline#send(HttpPipelineCallContext)} and it's response.
     *
     * The default HttpClient {@link HttpClient#createDefault()} will be used to write request to wire and
     * receive response from wire.
     *
     * @param pipelinePolicies pipeline policies in the order they need to applied, a copy of this list
     *                         will be made so changing the original list after the creation of pipeline
     *                         will not mutate the pipeline
     */
    public HttpPipeline(List<HttpPipelinePolicy> pipelinePolicies) {
        this(HttpClient.createDefault(), pipelinePolicies);
    }

    /**
     * Get the policies in the pipeline.
     *
     * @return policies in the pipeline
     */
    public HttpPipelinePolicy[] pipelinePolicies() {
        return Arrays.copyOf(this.pipelinePolicies, this.pipelinePolicies.length);
    }

    /**
     * Get the {@link HttpClient} associated with the pipeline.
     *
     * @return the {@link HttpClient} associated with the pipeline
     */
    public HttpClient httpClient() {
        return this.httpClient;
    }

    /**
     * Creates a new context local to the provided http request.
     *
     * @param httpRequest the request for a context needs to be created
     * @return the request context
     */
    public HttpPipelineCallContext newContext(HttpRequest httpRequest) {
        return new HttpPipelineCallContext(httpRequest);
    }

    /**
     * Wraps the request in a context and send it through pipeline.
     *
     * @param request the request
     * @return a publisher upon subscription flows the context through policies, sends the request and emits response upon completion
     */
    public Mono<HttpResponse> send(HttpRequest request) {
        return this.send(this.newContext(request));
    }

    /**
     * Sends the context (containing request) through pipeline.
     *
     * @param context the request context
     * @return a publisher upon subscription flows the context through policies, sends the request and emits response upon completion
     */
    public Mono<HttpResponse> send(HttpPipelineCallContext context) {
        // Return deferred to mono for complete lazy behaviour.
        //
        return Mono.defer(() -> {
            HttpPipelineNextPolicy next = new HttpPipelineNextPolicy(this, context);
            return next.process();
        });
    }
}
