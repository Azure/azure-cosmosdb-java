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
package com.microsoft.azure.cosmos.changefeed.internal;

import com.microsoft.azure.cosmos.CosmosContainer;
import com.microsoft.azure.cosmos.changefeed.ChangeFeedObserver;
import com.microsoft.azure.cosmos.changefeed.ChangeFeedObserverFactory;
import com.microsoft.azure.cosmos.changefeed.ChangeFeedProcessor;
import com.microsoft.azure.cosmos.changefeed.ChangeFeedProcessorOptions;
import com.microsoft.azure.cosmos.changefeed.ContainerInfo;
import com.microsoft.azure.cosmos.changefeed.HealthMonitor;
import com.microsoft.azure.cosmos.changefeed.LeaseStoreManager;
import com.microsoft.azure.cosmos.changefeed.PartitionLoadBalancingStrategy;
import com.microsoft.azure.cosmos.changefeed.PartitionProcessorFactory;
import reactor.core.publisher.Mono;

import java.util.concurrent.ExecutorService;

/**
 * Helper class to build {@link ChangeFeedProcessor} instances
 * as logical representation of the Azure Cosmos DB database service.
 *
 * <pre>
 * {@code
 *  ChangeFeedProcessor.Builder()
 *       .withHostName("SampleHost")
 *       .withFeedCollection(
 *           new DocumentCollectionInfo()
 *               .withDatabaseName("DatabaseName")
 *               .withCollectionName("MonitoredCollectionName")
 *               .withUri(new URI("https://sampleservice.documents.azure.com:443/"))
 *               .withMasterKey("-- the auth key"))
 *       .withLeaseCollection(
 *           new DocumentCollectionInfo()
 *               .withDatabaseName("DatabaseName")
 *               .withCollectionName("leases")
 *               .withUri(new URI("https://sampleservice.documents.azure.com:443/"))
 *               .withMasterKey("-- the auth key"))
 *       .withChangeFeedObserver(SampleObserverImpl.class)
 *       .build();
 * }
 * </pre>
 */
public class ChangeFeedProcessorBuilderImpl implements ChangeFeedProcessor.BuilderDefinition {
    @Override
    public ChangeFeedProcessor.BuilderDefinition withHostName(String hostName) {
        return null;
    }

    @Override
    public ChangeFeedProcessor.BuilderDefinition withFeedCollection(ContainerInfo feedCollectionLocation) {
        return null;
    }

    @Override
    public ChangeFeedProcessor.BuilderDefinition withFeedContainerClient(CosmosContainer feedContainerClient) {
        return null;
    }

    @Override
    public ChangeFeedProcessor.BuilderDefinition withProcessorOptions(ChangeFeedProcessorOptions changeFeedProcessorOptions) {
        return null;
    }

    @Override
    public ChangeFeedProcessor.BuilderDefinition withChangeFeedObserverFactory(ChangeFeedObserverFactory observerFactory) {
        return null;
    }

    @Override
    public ChangeFeedProcessor.BuilderDefinition withChangeFeedObserver(Class<? extends ChangeFeedObserver> type) {
        return null;
    }

    @Override
    public ChangeFeedProcessor.BuilderDefinition withDatabaseResourceId(String databaseResourceId) {
        return null;
    }

    @Override
    public ChangeFeedProcessor.BuilderDefinition withCollectionResourceId(String collectionResourceId) {
        return null;
    }

    @Override
    public ChangeFeedProcessor.BuilderDefinition withLeaseCollection(ContainerInfo leaseCollectionLocation) {
        return null;
    }

    @Override
    public ChangeFeedProcessor.BuilderDefinition withLeaseDocumentClient(CosmosContainer leaseCosmosClient) {
        return null;
    }

    @Override
    public ChangeFeedProcessor.BuilderDefinition withPartitionLoadBalancingStrategy(PartitionLoadBalancingStrategy loadBalancingStrategy) {
        return null;
    }

    @Override
    public ChangeFeedProcessor.BuilderDefinition withPartitionProcessorFactory(PartitionProcessorFactory partitionProcessorFactory) {
        return null;
    }

    @Override
    public ChangeFeedProcessor.BuilderDefinition withLeaseStoreManager(LeaseStoreManager leaseStoreManager) {
        return null;
    }

    @Override
    public ChangeFeedProcessor.BuilderDefinition withHealthMonitor(HealthMonitor healthMonitor) {
        return null;
    }

    @Override
    public ChangeFeedProcessor.BuilderDefinition withHealthMonitor(ExecutorService executorService) {
        return null;
    }

    @Override
    public Mono<ChangeFeedProcessor> build() {
        return null;
    }

}
