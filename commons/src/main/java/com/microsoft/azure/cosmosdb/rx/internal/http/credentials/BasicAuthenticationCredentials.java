// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.azure.cosmosdb.rx.internal.http.credentials;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Basic Auth credentials for use with a REST Service Client.
 */
public class BasicAuthenticationCredentials implements ServiceClientCredentials {
    /**
     * Basic auth user name.
     */
    private String userName;

    /**
     * Basic auth password.
     */
    private String password;

    /**
     * Creates a basic authentication credential.
     *
     * @param userName basic auth user name
     * @param password basic auth password
     */
    public BasicAuthenticationCredentials(String userName, String password) {
        this.userName = userName;
        this.password = password;
    }

    @Override
    public String authorizationHeaderValue(String uri) {
        String credential = userName + ":" + password;
        String encodedCredential;
        encodedCredential = Base64.getEncoder().encodeToString(credential.getBytes(StandardCharsets.UTF_8));

        return "Basic " + encodedCredential;
    }
}
