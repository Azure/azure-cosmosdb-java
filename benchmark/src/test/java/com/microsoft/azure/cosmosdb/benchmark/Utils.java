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

package com.microsoft.azure.cosmosdb.benchmark;

import com.microsoft.azure.cosmos.CosmosClient;
import com.microsoft.azure.cosmos.CosmosDatabase;
import com.microsoft.azure.cosmos.CosmosDatabaseResponse;
import com.microsoft.azure.cosmos.CosmosDatabaseSettings;
import com.microsoft.azure.cosmos.CosmosDatabaseForTest;
import com.microsoft.azure.cosmosdb.ConnectionPolicy;
import com.microsoft.azure.cosmosdb.Database;
import com.microsoft.azure.cosmosdb.DocumentCollection;
import com.microsoft.azure.cosmosdb.FeedResponse;
import com.microsoft.azure.cosmosdb.RetryOptions;
import com.microsoft.azure.cosmosdb.SqlQuerySpec;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import com.microsoft.azure.cosmosdb.rx.TestConfigurations;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class Utils {
    public static CosmosClient housekeepingClient() {
        ConnectionPolicy connectionPolicy = new ConnectionPolicy();
        RetryOptions options = new RetryOptions();
        options.setMaxRetryAttemptsOnThrottledRequests(100);
        options.setMaxRetryWaitTimeInSeconds(60);
        connectionPolicy.setRetryOptions(options);
        return new CosmosClient.Builder().endpoint(TestConfigurations.HOST)
                .key(TestConfigurations.MASTER_KEY)
                .connectionPolicy(connectionPolicy)
                .build();
    }

    public static String getCollectionLink(Database db, DocumentCollection collection) {
        return "dbs/" + db.getId() + "/colls/" + collection;
    }

    public static CosmosDatabase createDatabaseForTest(CosmosClient client) {
        return CosmosDatabaseForTest.create(DatabaseManagerImpl.getInstance(client)).createdDatabase;
    }

    public static void safeCleanDatabases(CosmosClient client) {
        if (client != null) {
            CosmosDatabaseForTest.cleanupStaleTestDatabases(DatabaseManagerImpl.getInstance(client));
        }
    }

    public static void safeClean(CosmosDatabase database) {
        if (database != null) {
            database.delete().block();
        }
    }

    public static void safeClean(AsyncDocumentClient client, String databaseId) {
        if (client != null) {
            if (databaseId != null) {
                try {
                    client.deleteDatabase("/dbs/" + databaseId, null).toBlocking().single();
                } catch (Exception e) {
                }
            }
        }
    }

    public static String generateDatabaseId() {
        return CosmosDatabaseForTest.generateId();
    }

    public static void safeClose(CosmosClient client) {
        if (client != null) {
            client.close();
        }
    }

    private static class DatabaseManagerImpl implements CosmosDatabaseForTest.DatabaseManager {
        public static DatabaseManagerImpl getInstance(CosmosClient client) {
            return new DatabaseManagerImpl(client);
        }

        private final CosmosClient client;

        private DatabaseManagerImpl(CosmosClient client) {
            this.client = client;
        }

        @Override
        public Flux<FeedResponse<CosmosDatabaseSettings>> queryDatabases(SqlQuerySpec query) {
            return client.queryDatabases(query, null);
        }

        @Override
        public Mono<CosmosDatabaseResponse> createDatabase(CosmosDatabaseSettings databaseDefinition) {
            return client.createDatabase(databaseDefinition);
        }

        @Override
        public CosmosDatabase getDatabase(String id) {
            return client.getDatabase(id);
        }
    }
}
