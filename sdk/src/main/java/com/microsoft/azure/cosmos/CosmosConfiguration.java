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
package com.microsoft.azure.cosmos;

import com.microsoft.azure.cosmosdb.ConnectionPolicy;
import com.microsoft.azure.cosmosdb.ConsistencyLevel;
import com.microsoft.azure.cosmosdb.Permission;
import com.microsoft.azure.cosmosdb.TokenResolver;
import com.microsoft.azure.cosmosdb.rx.internal.Configs;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

public class CosmosConfiguration {
    private Configs configs;
    private URI serviceEndpoint;
    private String keyOrResourceToken;
    private ConnectionPolicy connectionPolicy;
    private ConsistencyLevel desiredConsistencyLevel;
    private List<Permission> permissions;
    private TokenResolver tokenResolver;

    private CosmosConfiguration(Builder builder) {
        this.configs = builder.configs;
        this.serviceEndpoint = builder.serviceEndpoint;
        this.keyOrResourceToken = builder.keyOrResourceToken;
        this.connectionPolicy = builder.connectionPolicy;
        this.desiredConsistencyLevel = builder.desiredConsistencyLevel;
        this.permissions = builder.permissions;
        this.tokenResolver = builder.tokenResolver;
    }

    public static class Builder {

        Configs configs = new Configs();
        URI serviceEndpoint;
        String keyOrResourceToken;
        ConnectionPolicy connectionPolicy; //can set a default value here
        ConsistencyLevel desiredConsistencyLevel; //can set a default value here
        List<Permission> permissions; //can set a default value here
        TokenResolver tokenResolver;

        public Builder withServiceEndpoint(String serviceEndpoint) {
            try {
                this.serviceEndpoint = new URI(serviceEndpoint);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException(e.getMessage());
            }
            return this;
        }

        /**
         * This method will take either key or resource token and perform authentication
         * for accessing resource.
         * 
         * @param keyOrResourceToken key or resourceToken for authentication .
         * @return current Builder.
         */
        public Builder withKeyOrResourceToken(String keyOrResourceToken) {
            this.keyOrResourceToken = keyOrResourceToken;
            return this;
        }

        /**
         * This method will accept the permission list , which contains the
         * resource tokens needed to access resources.
         *
         * @param permissions Permission list for authentication.
         * @return current Builder.
         */
        public Builder withPermissions(List<Permission> permissions) {
            this.permissions = permissions;
            return this;
        }

        /**
         * This method accepts the (@link ConsistencyLevel) to be used
         * @param desiredConsistencyLevel (@link ConsistencyLevel)
         * @return current Builder
         */
        public Builder withConsistencyLevel(ConsistencyLevel desiredConsistencyLevel) {
            this.desiredConsistencyLevel = desiredConsistencyLevel;
            return this;
        }

        /**
         * The (@link ConnectionPolicy) to be used
         * @param connectionPolicy (@link ConnectionPolicy)
         * @return current Builder
         */
        public Builder withConnectionPolicy(ConnectionPolicy connectionPolicy) {
            this.connectionPolicy = connectionPolicy;
            return this;
        }

        /**
         * The (@link Configs) to be used
         * @param configs (@link configs)
         * @return current Builder
         */
        public Builder withConfigs(Configs configs) {
            this.configs = configs;
            return this;
        }

        /**
         * This method will accept functional interface TokenResolver which helps in generation authorization
         * token per request. AsyncDocumentClient can be successfully initialized with this API without passing any MasterKey, ResourceToken or PermissionFeed.
         * @param tokenResolver The tokenResolver
         * @return current Builder.
         */
        public Builder withTokenResolver(TokenResolver tokenResolver) {
            this.tokenResolver = tokenResolver;
            return this;
        }

        private void ifThrowIllegalArgException(boolean value, String error) {
            if (value) {
                throw new IllegalArgumentException(error);
            }
        }

        /**
         * Builds a cosmos configuration object with the provided settings
         * @return CosmosConfiguration
         */
        public CosmosConfiguration build() {

            ifThrowIllegalArgException(this.serviceEndpoint == null, "cannot build client without service endpoint");
            ifThrowIllegalArgException(
                    this.keyOrResourceToken == null && (permissions == null || permissions.isEmpty()),
                    "cannot build client without key or resource token");

            return new CosmosConfiguration(this);
        }
        
        public URI getServiceEndpoint() {
            return serviceEndpoint;
        }

        public String getKeyOrResourceToken() {
            return keyOrResourceToken;
        }

        public ConnectionPolicy getConnectionPolicy() {
            return connectionPolicy;
        }

        public Configs getConfigs() {
            return configs;
        }

        /**
         * Gets the consistency level
         * @return the (@link ConsistencyLevel)
         */
        public ConsistencyLevel getDesiredConsistencyLevel() {
            return desiredConsistencyLevel;
        }

        /**
         * Gets the permission list
         * @return the permission list
         */
        public List<Permission> getPermissions() {
            return permissions;
        }
        
        public TokenResolver getTokenResolver() {
            return tokenResolver;
        }
    }

    public URI getServiceEndpoint() {
        return serviceEndpoint;
    }

    public String getKeyOrResourceToken() {
        return keyOrResourceToken;
    }

    public ConnectionPolicy getConnectionPolicy() {
        return connectionPolicy;
    }

    public Configs getConfigs() {
        return configs;
    }

    /**
     * Gets the consistency level
     * @return the (@link ConsistencyLevel)
     */
    public ConsistencyLevel getDesiredConsistencyLevel() {
        return desiredConsistencyLevel;
    }

    /**
     * Gets the permission list
     * @return the permission list
     */
    public List<Permission> getPermissions() {
        return permissions;
    }
    
    public TokenResolver getTokenResolver() {
        return tokenResolver;
    }
}
