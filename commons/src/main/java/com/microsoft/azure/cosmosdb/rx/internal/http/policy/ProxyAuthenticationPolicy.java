// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.azure.cosmosdb.rx.internal.http.policy;

import com.microsoft.azure.cosmosdb.rx.internal.http.Base64Util;
import com.microsoft.azure.cosmosdb.rx.internal.http.HttpPipelineCallContext;
import com.microsoft.azure.cosmosdb.rx.internal.http.HttpPipelineNextPolicy;
import com.microsoft.azure.cosmosdb.rx.internal.http.HttpResponse;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;

/**
 * The Pipeline policy that adds basic proxy authentication to outgoing HTTP requests.
 */
public class ProxyAuthenticationPolicy implements HttpPipelinePolicy {
    private final String username;
    private final String password;

    /**
     * Creates a ProxyAuthenticationPolicy.
     *
     * @param username the username for authentication.
     * @param password the password for authentication.
     */
    public ProxyAuthenticationPolicy(String username, String password) {
        this.username = username;
        this.password = password;
    }

    @Override
    public Mono<HttpResponse> process(HttpPipelineCallContext context, HttpPipelineNextPolicy next) {
        String auth = username + ":" + password;
        String encodedAuth = Base64Util.encodeToString(auth.getBytes(StandardCharsets.UTF_8));
        context.httpRequest().withHeader("Proxy-Authentication", "Basic " + encodedAuth);
        return next.process();
    }
}
