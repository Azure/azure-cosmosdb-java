/**
 * The MIT License (MIT)
 * Copyright (c) 2018 Microsoft Corporation
 * <p>
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * <p>
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * <p>
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.microsoft.azure.cosmosdb.mapper;

import com.microsoft.azure.cosmosdb.Database;
import com.microsoft.azure.cosmosdb.DocumentClientException;
import com.microsoft.azure.cosmosdb.DocumentCollection;
import com.microsoft.azure.cosmosdb.ResourceResponse;
import com.microsoft.azure.cosmosdb.SqlParameter;
import com.microsoft.azure.cosmosdb.SqlParameterCollection;
import com.microsoft.azure.cosmosdb.SqlQuerySpec;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import rx.Observable;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Logger;

/**
 * The default implementation of {@link MappingManager}
 */
final class DefaultMappingManager implements MappingManager {

    private final AsyncDocumentClient client;

    private static final Logger LOGGER = Logger.getLogger(DefaultMappingManager.class.getName());

    private final Set<String> databases = new TreeSet<>();

    private final Set<EntityMetadata> metadatas = new HashSet<>();


    DefaultMappingManager(AsyncDocumentClient client) {
        this.client = client;
    }

    @Override
    public <T> Mapper<T> mapper(Class<T> entityClass) {
        Objects.requireNonNull(entityClass, "entity class is required");
        EntityMetadata metadata = EntityMetadata.of(entityClass);

        if (metadatas.contains(metadata)) {
            LOGGER.info("Entity already checked");
        } else {
            metadatas.add(metadata);
            createDatabase(metadata);
            createCollection(metadata);
        }
        return new DefaultMapper<>(entityClass, client, metadata);
    }


    private void createCollection(EntityMetadata metadata) {


        String databaseName = metadata.getDatabaseName();
        String collectionName = metadata.getCollectionName();

        String databaseLink = metadata.getCollectionLink();

        client.queryCollections(databaseLink,
                new SqlQuerySpec("SELECT * FROM r where r.id = @id",
                        new SqlParameterCollection(
                                new SqlParameter("@id", collectionName))), null)
                .single()
                .flatMap(page -> {
                    if (page.getResults().isEmpty()) {
                        DocumentCollection collection = new DocumentCollection();
                        collection.setId(collectionName);
                        LOGGER.info("Creating collection " + collectionName);
                        return client.createCollection(databaseLink, collection, null);
                    } else {
                        LOGGER.info("Collection " + collectionName + "already exists");
                        return Observable.empty();
                    }
                }).toCompletable().await();

        LOGGER.info("Checking collection " + collectionName + " completed!\n");
    }

    private void createDatabase(EntityMetadata metadata) {


        if (databases.contains(metadata.getDatabaseName())) {
            LOGGER.info("Database already checked");
            return;
        }

        String databaseName = metadata.getDatabaseName();
        databases.add(databaseName);
        String databaseLink = String.format("/dbs/%s", databaseName);
        Observable<ResourceResponse<Database>> databaseReadObs =
                client.readDatabase(databaseLink, null);

        Observable<ResourceResponse<Database>> databaseExistenceObs =
                databaseReadObs
                        .doOnNext(x -> {
                            System.out.println("database " + databaseName + " already exists.");
                        })
                        .onErrorResumeNext(
                                e -> {

                                    if (e instanceof DocumentClientException) {
                                        DocumentClientException de = (DocumentClientException) e;
                                        // if database
                                        if (de.getStatusCode() == 404) {
                                            LOGGER.info("database " + databaseName + " doesn't existed, creating it...");

                                            Database dbDefinition = new Database();
                                            dbDefinition.setId(databaseName);

                                            return client.createDatabase(dbDefinition, null);
                                        }
                                    }

                                    LOGGER.info("Reading database " + databaseName + " failed.");
                                    return Observable.error(e);
                                });


        databaseExistenceObs.toCompletable().await();
        LOGGER.info("Checking database " + databaseName + " completed!\n");
    }
}
