// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.azure.cosmosdb.rx.internal.http.policy;

import com.microsoft.azure.cosmosdb.rx.internal.http.HttpPipelineCallContext;
import com.microsoft.azure.cosmosdb.rx.internal.http.HttpPipelineNextPolicy;
import com.microsoft.azure.cosmosdb.rx.internal.http.HttpResponse;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/**
 * The Pipeline policy that adds Date header in RFC 1123 format when sending an HTTP request.
 */
public class AddDatePolicy implements HttpPipelinePolicy {
    private final DateTimeFormatter format = DateTimeFormatter
            .ofPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'")
            .withZone(ZoneId.of("UTC"))
            .withLocale(Locale.US);

    @Override
    public Mono<HttpResponse> process(HttpPipelineCallContext context, HttpPipelineNextPolicy next) {
        return Mono.defer(() -> {
            context.httpRequest().headers().set("Date", format.format(OffsetDateTime.now()));
            return next.process();
        });
    }
}
