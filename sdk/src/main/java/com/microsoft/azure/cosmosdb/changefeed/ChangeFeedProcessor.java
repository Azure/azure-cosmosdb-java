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
package com.microsoft.azure.cosmosdb.changefeed;

import com.microsoft.azure.cosmos.CosmosContainer;
import com.microsoft.azure.cosmosdb.changefeed.internal.ChangeFeedProcessorBuilderImpl;
import reactor.core.publisher.Mono;

import java.util.concurrent.ExecutorService;

/**
 * Simple host for distributing change feed events across observers and thus allowing these observers scale.
 * It distributes the load across its instances and allows dynamic scaling:
 *   - Partitions in partitioned collections are distributed across instances/observers.
 *   - New instance takes leases from existing instances to make distribution equal.
 *   - If an instance dies, the leases are distributed across remaining instances.
 * It's useful for scenario when partition count is high so that one host/VM is not capable of processing that many change feed events.
 * Client application needs to implement {@link ChangeFeedObserver} and register processor implementation with {@link ChangeFeedProcessor}.
 * <p>
 * It uses auxiliary document collection for managing leases for a partition.
 * Every EventProcessorHost instance is performing the following two tasks:
 *     1) Renew Leases: It keeps track of leases currently owned by the host and continuously keeps on renewing the leases.
 *     2) Acquire Leases: Each instance continuously polls all leases to check if there are any leases it should acquire
 *     for the system to get into balanced state.
 * <p>
 * {@code
 * ChangeFeedProcessor changeFeedProcessor = ChangeFeedProcessor.Builder()
 *     .withHostName(hostName)
 *     .withFeedCollection(
 *     new DocumentCollectionInfo()
 *         .withDatabaseName("COSMOSDB_DATABASE")
 *         .withCollectionName("COSMOSDB_COLLECTION")
 *         .withUri("COSMOSDB_ENDPOINT")
 *         .withMasterKey("COSMOSDB_SECRET"))
 *     .withLeaseCollection(
 *     new DocumentCollectionInfo()
 *         .withDatabaseName("COSMOSDB_DATABASE")
 *         .withCollectionName("COSMOSDB_COLLECTION" + "-lease")
 *         .withUri("COSMOSDB_ENDPOINT")
 *         .withMasterKey("COSMOSDB_SECRET"))
 *     .withChangeFeedObserver(SampleObserverImpl.class)
 *     .build();
 * }
 */
public interface ChangeFeedProcessor {

    /**
     * Start listening for changes asynchronously.
     *
     * @return a representation of the deferred computation of this call.
     */
    Mono<Void> startAsync();

    /**
     * Start listening for changes.
     */
    void start();

    /**
     * Stops listening for changes asynchronously.
     *
     * @return a representation of the deferred computation of this call.
     */
    Mono<Void> stopAsync();

    /**
     * Stops listening for changes.
     */
    void stop();

    /**
     * Helper static method to build {@link ChangeFeedProcessor} instances
     * as logical representation of the Azure Cosmos DB database service.
     * <p>
     * {@code
     *
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
     *
     * @return a builder definition instance.
     */
    static BuilderDefinition Builder() {
        return new ChangeFeedProcessorBuilderImpl();
    }

    interface BuilderDefinition {
        /**
         * Sets the host name.
         *
         * @param hostName the name to be used for the host. When using multiple hosts, each host must have a unique name.
         * @return current Builder.
         */
        BuilderDefinition withHostName(String hostName);

        /**
         * Sets the {@link ContainerInfo} of the collection to listen for changes.
         *
         * @param feedCollectionLocation the {@link ContainerInfo} of the collection to listen for changes.
         * @return current Builder.
         */
        BuilderDefinition withFeedCollection(ContainerInfo feedCollectionLocation);

        /**
         * Sets and existing {@link CosmosContainer} to be used to read from the monitored collection.
         *
         * @param feedContainerClient the instance of {@link CosmosContainer} to be used.
         * @return current Builder.
         */
        BuilderDefinition withFeedContainerClient(CosmosContainer feedContainerClient);

        /**
         * Sets the {@link ChangeFeedProcessorOptions} to be used.
         *
         * @param changeFeedProcessorOptions the change feed processor options to use.
         * @return current Builder.
         */
        BuilderDefinition withProcessorOptions(ChangeFeedProcessorOptions changeFeedProcessorOptions);

        /**
         * Sets the {@link ChangeFeedObserverFactory} to be used to generate {@link ChangeFeedObserver}
         *
         * @param observerFactory The instance of {@link ChangeFeedObserverFactory} to use.
         * @return current Builder.
         */
        BuilderDefinition withChangeFeedObserverFactory(ChangeFeedObserverFactory observerFactory);

        /**
         * Sets an existing {@link ChangeFeedObserver} type to be used by a {@link ChangeFeedObserverFactory} to process changes.
         *
         * @param type the type of {@link ChangeFeedObserver} to be used.
         * @return current Builder.
         */
        BuilderDefinition withChangeFeedObserver(Class<? extends ChangeFeedObserver> type);

        /**
         * Sets the database resource ID of the monitored collection.
         *
         * @param databaseResourceId the database resource ID of the monitored collection.
         * @return current Builder.
         */
        BuilderDefinition withDatabaseResourceId(String databaseResourceId);

        /**
         * Sets the collection resource ID of the monitored collection.
         *
         * @param collectionResourceId the collection resource ID of the monitored collection.
         * @return current Builder.
         */
        BuilderDefinition withCollectionResourceId(String collectionResourceId);

        /**
         * Sets the {@link ContainerInfo} of the collection to use for leases.
         *
         * @param leaseCollectionLocation the {@link ContainerInfo} of the collection to use for leases.
         * @return current Builder.
         */
        BuilderDefinition withLeaseCollection(ContainerInfo leaseCollectionLocation);

        /**
         * Sets an existing {@link CosmosContainer} to be used to read from the leases collection.
         *
         * @param leaseCosmosClient the instance of {@link CosmosContainer} to use.
         * @return current Builder.
         */
        BuilderDefinition withLeaseDocumentClient(CosmosContainer leaseCosmosClient);

        /**
         * Sets the {@link PartitionLoadBalancingStrategy} to be used for partition load balancing.
         *
         * @param loadBalancingStrategy the {@link PartitionLoadBalancingStrategy} to be used for partition load balancing.
         * @return current Builder.
         */
        BuilderDefinition withPartitionLoadBalancingStrategy(PartitionLoadBalancingStrategy loadBalancingStrategy);

        /**
         * Sets the {@link PartitionProcessorFactory} to be used to create {@link PartitionProcessor} for partition processing.
         *
         * @param partitionProcessorFactory the instance of {@link PartitionProcessorFactory} to use.
         * @return current Builder.
         */
        BuilderDefinition withPartitionProcessorFactory(PartitionProcessorFactory partitionProcessorFactory);

        /**
         * Sets the {@link LeaseStoreManager} to be used to manage leases.
         *
         * @param leaseStoreManager the instance of {@link LeaseStoreManager} to use.
         * @return current Builder.
         */
        BuilderDefinition withLeaseStoreManager(LeaseStoreManager leaseStoreManager);

        /**
         * Sets the {@link HealthMonitor} to be used to monitor unhealthiness situation.
         *
         * @param healthMonitor The instance of {@link HealthMonitor} to use.
         * @return current Builder.
         */
        BuilderDefinition withHealthMonitor(HealthMonitor healthMonitor);

        /**
         * Sets the {@link ExecutorService} to be used to control the thread pool.
         *
         * @param executorService The instance of {@link ExecutorService} to use.
         * @return current Builder.
         */
        BuilderDefinition withHealthMonitor(ExecutorService executorService);

        /**
         * Builds a new instance of the {@link ChangeFeedProcessor} with the specified configuration asynchronously.
         *
         * @return an instance of {@link ChangeFeedProcessor}.
         */
        Mono<ChangeFeedProcessor> buildAsync();

        /**
         * Builds a new instance of the {@link ChangeFeedProcessor} with the specified configuration.
         *
         * @return an instance of {@link ChangeFeedProcessor}.
         */
        ChangeFeedProcessor build();
    }
}
