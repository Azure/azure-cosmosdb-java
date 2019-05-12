// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.azure.cosmosdb.rx.internal.http.policy;

import com.microsoft.azure.cosmosdb.rx.internal.http.HttpPipelineCallContext;
import com.microsoft.azure.cosmosdb.rx.internal.http.HttpPipelineNextPolicy;
import com.microsoft.azure.cosmosdb.rx.internal.http.HttpResponse;
import com.microsoft.azure.cosmosdb.rx.internal.http.credentials.AsyncServiceClientCredentials;
import reactor.core.publisher.Mono;

import java.util.Objects;

/**
 * Creates a policy which adds credentials from {@link AsyncServiceClientCredentials} to the 'Authorization' header of
 * each HTTP request.
 */
public class AsyncCredentialsPolicy implements HttpPipelinePolicy {
    private final AsyncServiceClientCredentials credentials;

    /**
     * Creates an {@link AsyncCredentialsPolicy} that authenticates HTTP requests using the given {@code credentials}.
     *
     * @param credentials The credentials to use for authentication.
     */
    public AsyncCredentialsPolicy(AsyncServiceClientCredentials credentials) {
        Objects.requireNonNull(credentials);
        this.credentials = credentials;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Mono<HttpResponse> process(HttpPipelineCallContext context, HttpPipelineNextPolicy next) {
        return credentials.authorizationHeaderValueAsync(context.httpRequest())
            .flatMap(token -> {
                context.httpRequest().headers().set("Authorization", token);
                return next.process();
            });
    }
}
