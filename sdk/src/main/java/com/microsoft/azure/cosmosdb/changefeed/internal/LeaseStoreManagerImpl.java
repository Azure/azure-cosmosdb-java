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
package com.microsoft.azure.cosmosdb.changefeed.internal;

import com.microsoft.azure.cosmosdb.changefeed.ChangeFeedContextClient;
import com.microsoft.azure.cosmosdb.changefeed.ContainerInfo;
import com.microsoft.azure.cosmosdb.changefeed.Lease;
import com.microsoft.azure.cosmosdb.changefeed.LeaseStoreManager;
import com.microsoft.azure.cosmosdb.changefeed.RequestOptionsFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * Provides flexible way to build lease manager constructor parameters.
 * For the actual creation of lease manager instance, delegates to lease manager factory.
 */
public class LeaseStoreManagerImpl implements LeaseStoreManager, LeaseStoreManager.LeaseStoreManagerBuilderDefinition {
    @Override
    public LeaseStoreManagerBuilderDefinition withLeaseCollection(ContainerInfo leaseCollectionLocation) {
        return null;
    }

    @Override
    public LeaseStoreManagerBuilderDefinition withLeaseDocumentClient(ChangeFeedContextClient leaseDocumentClient) {
        return null;
    }

    @Override
    public LeaseStoreManagerBuilderDefinition withLeasePrefix(String leasePrefix) {
        return null;
    }

    @Override
    public LeaseStoreManagerBuilderDefinition withLeaseCollectionLink(String leaseCollectionLink) {
        return null;
    }

    @Override
    public LeaseStoreManagerBuilderDefinition withRequestOptionsFactory(RequestOptionsFactory requestOptionsFactory) {
        return null;
    }

    @Override
    public LeaseStoreManagerBuilderDefinition withHostName(String hostName) {
        return null;
    }

    @Override
    public LeaseStoreManager build() {
        return null;
    }

    @Override
    public Flux<LeaseStoreManager> buildAsync() {
        return null;
    }

    @Override
    public Flux<Lease> getAllLeasesAsync() {
        return null;
    }

    @Override
    public Flux<Lease> getOwnedLeasesAsync() {
        return null;
    }

    @Override
    public Flux<Lease> createLeaseIfNotExistAsync(String leaseToken, String continuationToken) {
        return null;
    }

    @Override
    public Mono<Void> deleteAsync(Lease lease) {
        return null;
    }

    @Override
    public Flux<Lease> acquireAsync(Lease lease) {
        return null;
    }

    @Override
    public Mono<Void> releaseAsync(Lease lease) {
        return null;
    }

    @Override
    public Flux<Lease> renewAsync(Lease lease) {
        return null;
    }

    @Override
    public Flux<Lease> updatePropertiesAsync(Lease leaseToUpdatePropertiesFrom) {
        return null;
    }

    @Override
    public Flux<Lease> checkpointAsync(Lease lease, String continuationToken) {
        return null;
    }

    @Override
    public Mono<Boolean> isInitializedAsync() {
        return null;
    }

    @Override
    public Mono<Void> markInitializedAsync() {
        return null;
    }

    @Override
    public Mono<Boolean> acquireInitializationLockAsync(Duration lockExpirationTime) {
        return null;
    }

    @Override
    public Mono<Boolean> releaseInitializationLockAsync() {
        return null;
    }
}
