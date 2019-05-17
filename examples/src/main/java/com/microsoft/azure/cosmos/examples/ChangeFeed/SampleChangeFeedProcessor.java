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
package com.microsoft.azure.cosmos.examples.ChangeFeed;

import com.microsoft.azure.cosmos.ChangeFeedProcessor;
import com.microsoft.azure.cosmos.CosmosClient;
import com.microsoft.azure.cosmos.CosmosConfiguration;
import com.microsoft.azure.cosmos.CosmosContainer;
import com.microsoft.azure.cosmos.CosmosContainerRequestOptions;
import com.microsoft.azure.cosmos.CosmosContainerResponse;
import com.microsoft.azure.cosmos.CosmosContainerSettings;
import com.microsoft.azure.cosmos.CosmosDatabase;
import com.microsoft.azure.cosmos.CosmosItem;
import com.microsoft.azure.cosmosdb.ConnectionPolicy;
import com.microsoft.azure.cosmosdb.ConsistencyLevel;
import com.microsoft.azure.cosmosdb.DocumentClientException;
import com.microsoft.azure.cosmosdb.SerializationFormattingPolicy;
import org.apache.commons.lang3.RandomStringUtils;

import java.time.Duration;

/**
 * Sample class to test the implementation.
 *
 */
public class SampleChangeFeedProcessor {

    public static final String DATABASE_NAME = "db_" + RandomStringUtils.randomAlphabetic(7);
    public static final String COLLECTION_NAME = "coll_" + RandomStringUtils.randomAlphabetic(7);

    public static void main (String[]args) {
        System.out.println("BEGIN Sample");

        try {

            System.out.println("-->Create DocumentClient");
            CosmosClient client = getCosmosClient();

            System.out.println("-->Create sample's database: " + DATABASE_NAME);
            CosmosDatabase cosmosDatabase = createNewDatabase(client, DATABASE_NAME);

            System.out.println("-->Create container for documents: " + COLLECTION_NAME);
            CosmosContainer feedContainer = createNewCollection(client, DATABASE_NAME, COLLECTION_NAME);

            System.out.println("-->Create container for lease: " + COLLECTION_NAME + "-leases");
            CosmosContainer leaseContainer = createNewLeaseCollection(client, DATABASE_NAME, COLLECTION_NAME + "-leases");

            ChangeFeedProcessor changeFeedProcessor1 = getChangeFeedProcessor("SampleHost_1", feedContainer, leaseContainer);

            changeFeedProcessor1.start().block();

            createNewDocuments(feedContainer, 10, Duration.ofSeconds(3));

            Thread.sleep(15000);

            changeFeedProcessor1.stop().block();

            System.out.println("-->Delete sample's database: " + DATABASE_NAME);
            deleteDatabase(cosmosDatabase);

            Thread.sleep(15000);

            client.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("END Sample");
        System.exit(0);
    }

    public static ChangeFeedProcessor getChangeFeedProcessor(String hostName, CosmosContainer feedContainer, CosmosContainer leaseContainer) {
        return ChangeFeedProcessor.Builder()
            .withHostName(hostName)
            .withFeedContainerClient(feedContainer)
            .withLeaseContainerClient(leaseContainer)
            .withChangeFeedObserver(SampleObserverImpl.class)
            .build().block();
    }

    public static CosmosClient getCosmosClient() {
        try {
            return CosmosClient.create(new CosmosConfiguration.Builder()
                .withServiceEndpoint(SampleConfigurations.HOST)
                .withKeyOrResourceToken(SampleConfigurations.MASTER_KEY)
                .withConnectionPolicy(ConnectionPolicy.GetDefault())
                .withConsistencyLevel(ConsistencyLevel.Eventual)
                .build()
            );
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static CosmosDatabase createNewDatabase(CosmosClient client, String databaseName) {
        return client.createDatabaseIfNotExists(databaseName).block().getDatabase();
    }

    public static void deleteDatabase(CosmosDatabase cosmosDatabase) {
        cosmosDatabase.delete().block();
    }

    public static CosmosContainer createNewCollection(CosmosClient client, String databaseName, String collectionName) {
        CosmosDatabase databaseLink = client.getDatabase(databaseName);
        CosmosContainer collectionLink = databaseLink.getContainer(collectionName);
        CosmosContainerResponse containerResponse = null;

        try {
            containerResponse = collectionLink.read().block();

            if (containerResponse != null) {
                throw new IllegalArgumentException(String.format("Collection %s already exists in database %s.", collectionName, databaseName));
            }
        } catch (RuntimeException ex) {
            if (ex.getCause() instanceof DocumentClientException) {
                DocumentClientException documentClientException = (DocumentClientException) ex.getCause();

                if (documentClientException.getStatusCode() != 404) {
                    throw ex;
                }
            } else {
                throw ex;
            }
        }

        CosmosContainerSettings containerSettings = new CosmosContainerSettings(collectionName, "/id");

        CosmosContainerRequestOptions requestOptions = new CosmosContainerRequestOptions();
        requestOptions.setOfferThroughput(10000);

        containerResponse = databaseLink.createContainer(containerSettings, requestOptions).block();

        if (containerResponse == null) {
            throw new RuntimeException(String.format("Failed to create collection %s in database %s.", collectionName, databaseName));
        }

        return containerResponse.getContainer();
    }

    public static CosmosContainer createNewLeaseCollection(CosmosClient client, String databaseName, String leaseCollectionName) {
        CosmosDatabase databaseLink = client.getDatabase(databaseName);
        CosmosContainer leaseCollectionLink = databaseLink.getContainer(leaseCollectionName);
        CosmosContainerResponse leaseContainerResponse = null;

        try {
            leaseContainerResponse = leaseCollectionLink.read().block();

            if (leaseContainerResponse != null) {
                leaseCollectionLink.delete().block();

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }
        } catch (RuntimeException ex) {
            if (ex.getCause() instanceof DocumentClientException) {
                DocumentClientException documentClientException = (DocumentClientException) ex.getCause();

                if (documentClientException.getStatusCode() != 404) {
                    throw ex;
                }
            } else {
                throw ex;
            }
        }

        CosmosContainerSettings containerSettings = new CosmosContainerSettings(leaseCollectionName, "/id");
        CosmosContainerRequestOptions requestOptions = new CosmosContainerRequestOptions();
        requestOptions.setOfferThroughput(400);

        leaseContainerResponse = databaseLink.createContainer(containerSettings, requestOptions).block();

        if (leaseContainerResponse == null) {
            throw new RuntimeException(String.format("Failed to create collection %s in database %s.", leaseCollectionName, databaseName));
        }

        return leaseContainerResponse.getContainer();
    }

    public static void createNewDocuments(CosmosContainer containerClient, int count, Duration delay) {
        String suffix = RandomStringUtils.randomAlphabetic(10);
        for (int i = 0; i <= count; i++) {
            CosmosItem document = new CosmosItem();
            document.setId(String.format("0%d-%s", i, suffix));

            document = containerClient.createItem(document).block().getItem();

            System.out.println("---->DOCUMENT WRITE: " + document.toJson(SerializationFormattingPolicy.Indented));

            long remainingWork = delay.toMillis();
            try {
                while (remainingWork > 0) {
                    Thread.sleep(100);
                    remainingWork -= 100;
                }
            } catch (InterruptedException iex) {
                // exception caught
                break;
            }
        }
    }
}
