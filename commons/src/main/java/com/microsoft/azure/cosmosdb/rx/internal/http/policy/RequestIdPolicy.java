// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.azure.cosmosdb.rx.internal.http.policy;

import com.microsoft.azure.cosmosdb.rx.internal.http.HttpPipelineCallContext;
import com.microsoft.azure.cosmosdb.rx.internal.http.HttpPipelineNextPolicy;
import com.microsoft.azure.cosmosdb.rx.internal.http.HttpResponse;
import reactor.core.publisher.Mono;

import java.util.UUID;

/**
 * The Pipeline policy that puts a UUID in the request header. Azure uses the request id as
 * the unique identifier for the request.
 */
public class RequestIdPolicy implements HttpPipelinePolicy {
    private static final String REQUEST_ID_HEADER = "x-ms-client-request-id";

    @Override
    public Mono<HttpResponse> process(HttpPipelineCallContext context, HttpPipelineNextPolicy next) {
        String requestId = context.httpRequest().headers().value(REQUEST_ID_HEADER);
        if (requestId == null) {
            context.httpRequest().headers().set(REQUEST_ID_HEADER, UUID.randomUUID().toString());
        }
        return next.process();
    }
}
