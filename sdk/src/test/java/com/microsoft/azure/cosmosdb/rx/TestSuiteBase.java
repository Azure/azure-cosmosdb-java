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
package com.microsoft.azure.cosmosdb.rx;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.cosmosdb.DataType;
import com.microsoft.azure.cosmosdb.DocumentClientException;
import com.microsoft.azure.cosmosdb.IncludedPath;
import com.microsoft.azure.cosmosdb.Index;
import com.microsoft.azure.cosmosdb.IndexingPolicy;
import com.microsoft.azure.cosmosdb.RetryOptions;
import com.microsoft.azure.cosmosdb.SqlQuerySpec;
import com.microsoft.azure.cosmosdb.Undefined;
import com.microsoft.azure.cosmosdb.internal.PathParser;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.Protocol;
import com.microsoft.azure.cosmosdb.rx.internal.Configs;

import io.reactivex.subscribers.TestSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import org.apache.commons.lang3.StringUtils;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.DataProvider;

import com.microsoft.azure.cosmos.CosmosClient;
import com.microsoft.azure.cosmos.CosmosClient.Builder;
import com.microsoft.azure.cosmos.CosmosContainer;
import com.microsoft.azure.cosmos.CosmosContainerRequestOptions;
import com.microsoft.azure.cosmos.CosmosContainerSettings;
import com.microsoft.azure.cosmos.CosmosDatabase;
import com.microsoft.azure.cosmos.CosmosDatabaseResponse;
import com.microsoft.azure.cosmos.CosmosDatabaseSettings;
import com.microsoft.azure.cosmos.CosmosItem;
import com.microsoft.azure.cosmos.CosmosItemResponse;
import com.microsoft.azure.cosmos.CosmosItemSettings;
import com.microsoft.azure.cosmos.CosmosRequestOptions;
import com.microsoft.azure.cosmos.CosmosResponse;
import com.microsoft.azure.cosmos.CosmosResponseValidator;
import com.microsoft.azure.cosmos.CosmosUser;
import com.microsoft.azure.cosmos.CosmosUserSettings;
import com.microsoft.azure.cosmos.DatabaseForTest;
import com.microsoft.azure.cosmosdb.CompositePath;
import com.microsoft.azure.cosmosdb.CompositePathSortOrder;
import com.microsoft.azure.cosmosdb.ConnectionMode;
import com.microsoft.azure.cosmosdb.ConnectionPolicy;
import com.microsoft.azure.cosmosdb.ConsistencyLevel;
import com.microsoft.azure.cosmosdb.FeedOptions;
import com.microsoft.azure.cosmosdb.FeedResponse;
import com.microsoft.azure.cosmosdb.PartitionKey;
import com.microsoft.azure.cosmosdb.PartitionKeyDefinition;
import com.microsoft.azure.cosmosdb.Resource;
import com.microsoft.azure.cosmosdb.ResourceResponse;

import org.testng.annotations.Test;
import rx.Observable;

public class TestSuiteBase {
    private static final int DEFAULT_BULK_INSERT_CONCURRENCY_LEVEL = 500;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    protected static Logger logger = LoggerFactory.getLogger(TestSuiteBase.class.getSimpleName());
    protected static final int TIMEOUT = 8000;
    protected static final int FEED_TIMEOUT = 12000;
    protected static final int SETUP_TIMEOUT = 30000;
    protected static final int SHUTDOWN_TIMEOUT = 12000;

    protected static final int SUITE_SETUP_TIMEOUT = 120000;
    protected static final int SUITE_SHUTDOWN_TIMEOUT = 60000;

    protected static final int WAIT_REPLICA_CATCH_UP_IN_MILLIS = 4000;

    protected int subscriberValidationTimeout = TIMEOUT;
    
    protected static ConsistencyLevel accountConsistency;

    protected Builder clientBuilder;

    protected static CosmosDatabase SHARED_DATABASE;
    protected static CosmosContainer SHARED_MULTI_PARTITION_COLLECTION;
    protected static CosmosContainer SHARED_MULTI_PARTITION_COLLECTION_WITH_COMPOSITE_AND_SPATIAL_INDEXES;

    protected TestSuiteBase() {
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        objectMapper.configure(JsonParser.Feature.ALLOW_TRAILING_COMMA, true);
        objectMapper.configure(JsonParser.Feature.STRICT_DUPLICATE_DETECTION, true);
        logger.debug("Initializing {} ...", this.getClass().getSimpleName());
    }

    @BeforeMethod(groups = { "simple", "long", "direct", "multi-master", "emulator", "non-emulator" })
    public void beforeMethod(Method method) {
        if (this.clientBuilder != null) {
            logger.info("Starting {}::{} using {} {} mode with {} consistency",
                        method.getDeclaringClass().getSimpleName(), method.getName(),
                        this.clientBuilder.getConnectionPolicy().getConnectionMode(),
                        this.clientBuilder.getConfigs().getProtocol(),
                        this.clientBuilder.getDesiredConsistencyLevel());
            return;
        }
        logger.info("Starting {}::{}", method.getDeclaringClass().getSimpleName(), method.getName());
    }

    @AfterMethod(groups = { "simple", "long", "direct", "multi-master", "emulator", "non-emulator" })
    public void afterMethod(Method m) {
        Test t = m.getAnnotation(Test.class);
        logger.info("Finished {}:{}.", m.getDeclaringClass().getSimpleName(), m.getName());
    }

    private static class DatabaseManagerImpl implements DatabaseForTest.DatabaseManager {
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

    @BeforeSuite(groups = { "simple", "long", "direct", "multi-master", "emulator", "non-emulator" }, timeOut = SUITE_SETUP_TIMEOUT)
    public static void beforeSuite() {
        logger.info("beforeSuite Started");
        CosmosClient houseKeepingClient = createGatewayHouseKeepingDocumentClient().build();
        DatabaseForTest dbForTest = DatabaseForTest.create(DatabaseManagerImpl.getInstance(houseKeepingClient));
        SHARED_DATABASE = dbForTest.createdDatabase;
        CosmosContainerRequestOptions options = new CosmosContainerRequestOptions();
        options.offerThroughput(10100);
        SHARED_MULTI_PARTITION_COLLECTION = createCollection(SHARED_DATABASE, getCollectionDefinitionWithRangeRangeIndex(), options);
        SHARED_MULTI_PARTITION_COLLECTION_WITH_COMPOSITE_AND_SPATIAL_INDEXES = createCollection(SHARED_DATABASE, getCollectionDefinitionMultiPartitionWithCompositeAndSpatialIndexes(), options);
    }

    @AfterSuite(groups = { "simple", "long", "direct", "multi-master", "emulator", "non-emulator" }, timeOut = SUITE_SHUTDOWN_TIMEOUT)
    public static void afterSuite() {
        logger.info("afterSuite Started");
        CosmosClient houseKeepingClient = createGatewayHouseKeepingDocumentClient().build();
        try {
            safeDeleteDatabase(SHARED_DATABASE);
            DatabaseForTest.cleanupStaleTestDatabases(DatabaseManagerImpl.getInstance(houseKeepingClient));
        } finally {
            safeClose(houseKeepingClient);
        }
    }

    protected static void truncateCollection(CosmosContainer cosmosContainer) {
        CosmosContainerSettings cosmosContainerSettings = cosmosContainer.read().block().getCosmosContainerSettings();
        String cosmosContainerId = cosmosContainerSettings.getId();
        logger.info("Truncating collection {} ...", cosmosContainerId);
        CosmosClient houseKeepingClient = createGatewayHouseKeepingDocumentClient().build();
        try {
            List<String> paths = cosmosContainerSettings.getPartitionKey().getPaths();
            FeedOptions options = new FeedOptions();
            options.setMaxDegreeOfParallelism(-1);
            options.setEnableCrossPartitionQuery(true);
            options.setMaxItemCount(100);

            logger.info("Truncating collection {} documents ...", cosmosContainer.getId());

            cosmosContainer.queryItems("SELECT * FROM root", options)
                    .flatMap(page -> Flux.fromIterable(page.getResults()))
                    .flatMap(doc -> {
                        
                        Object propertyValue = null;
                        if (paths != null && !paths.isEmpty()) {
                            List<String> pkPath = PathParser.getPathParts(paths.get(0));
                            propertyValue = doc.getObjectByPath(pkPath);
                            if (propertyValue == null) {
                                propertyValue = Undefined.Value();
                            }

                        }
                        cosmosContainer.getItem(doc.getId(), propertyValue).delete().block();
                        return null;
                    }).collectList().block();
            logger.info("Truncating collection {} triggers ...", cosmosContainerId);

            cosmosContainer.queryTriggers("SELECT * FROM root", options)
                    .flatMap(page -> Flux.fromIterable(page.getResults()))
                    .flatMap(trigger -> {
                        CosmosRequestOptions requestOptions = new CosmosRequestOptions();

//                    if (paths != null && !paths.isEmpty()) {
//                        Object propertyValue = trigger.getObjectByPath(PathParser.getPathParts(paths.get(0)));
//                        requestOptions.setPartitionKey(new PartitionKey(propertyValue));
//                    }

                        return cosmosContainer.getTrigger(trigger.toJson()).delete(requestOptions);
                    }).collectList().block();

            logger.info("Truncating collection {} storedProcedures ...", cosmosContainerId);

            cosmosContainer.queryStoredProcedures("SELECT * FROM root", options)
                    .flatMap(page -> Flux.fromIterable(page.getResults()))
                    .flatMap(storedProcedure -> {
                        CosmosRequestOptions requestOptions = new CosmosRequestOptions();

//                    if (paths != null && !paths.isEmpty()) {
//                        Object propertyValue = storedProcedure.getObjectByPath(PathParser.getPathParts(paths.get(0)));
//                        requestOptions.setPartitionKey(new PartitionKey(propertyValue));
//                    }

                        return cosmosContainer.getStoredProcedure(storedProcedure.toJson()).delete(requestOptions);
                    }).collectList().block();

            logger.info("Truncating collection {} udfs ...", cosmosContainerId);

            cosmosContainer.queryUserDefinedFunctions("SELECT * FROM root", options)
                    .flatMap(page -> Flux.fromIterable(page.getResults()))
                    .flatMap(udf -> {
                        CosmosRequestOptions requestOptions = new CosmosRequestOptions();

//                    if (paths != null && !paths.isEmpty()) {
//                        Object propertyValue = udf.getObjectByPath(PathParser.getPathParts(paths.get(0)));
//                        requestOptions.setPartitionKey(new PartitionKey(propertyValue));
//                    }

                        return cosmosContainer.getUserDefinedFunction(udf.toJson()).delete(requestOptions);
                    }).collectList().block();

        } finally {
            houseKeepingClient.close();
        }

        logger.info("Finished truncating collection {}.", cosmosContainerId);
    }

    protected static void waitIfNeededForReplicasToCatchUp(Builder clientBuilder) {
        switch (clientBuilder.getDesiredConsistencyLevel()) {
            case Eventual:
            case ConsistentPrefix:
                logger.info(" additional wait in Eventual mode so the replica catch up");
                // give times to replicas to catch up after a write
                try {
                    TimeUnit.MILLISECONDS.sleep(WAIT_REPLICA_CATCH_UP_IN_MILLIS);
                } catch (Exception e) {
                    logger.error("unexpected failure", e);
                }

            case Session:
            case BoundedStaleness:
            case Strong:
            default:
                break;
        }
    }

    public static CosmosContainer createCollection(String databaseId, CosmosContainerSettings cosmosContainerSettings,
            CosmosContainerRequestOptions options) {
        CosmosClient client = createGatewayHouseKeepingDocumentClient().build();
        try {
            return client.getDatabase(databaseId).createContainer(cosmosContainerSettings, options).block().getContainer();
        } finally {
            client.close();
        }
    }

    public static CosmosContainer createCollection(CosmosDatabase database, CosmosContainerSettings cosmosContainerSettings,
            CosmosContainerRequestOptions options) {
        return database.createContainer(cosmosContainerSettings, options).block().getContainer();
    }

    private static CosmosContainerSettings getCollectionDefinitionMultiPartitionWithCompositeAndSpatialIndexes() {
        final String NUMBER_FIELD = "numberField";
        final String STRING_FIELD = "stringField";
        final String NUMBER_FIELD_2 = "numberField2";
        final String STRING_FIELD_2 = "stringField2";
        final String BOOL_FIELD = "boolField";
        final String NULL_FIELD = "nullField";
        final String OBJECT_FIELD = "objectField";
        final String ARRAY_FIELD = "arrayField";
        final String SHORT_STRING_FIELD = "shortStringField";
        final String MEDIUM_STRING_FIELD = "mediumStringField";
        final String LONG_STRING_FIELD = "longStringField";
        final String PARTITION_KEY = "pk";

        PartitionKeyDefinition partitionKeyDefinition = new PartitionKeyDefinition();
        ArrayList<String> partitionKeyPaths = new ArrayList<String>();
        partitionKeyPaths.add("/" + PARTITION_KEY);
        partitionKeyDefinition.setPaths(partitionKeyPaths);

        CosmosContainerSettings cosmosContainerSettings = new CosmosContainerSettings(UUID.randomUUID().toString(), partitionKeyDefinition);

        IndexingPolicy indexingPolicy = new IndexingPolicy();
        Collection<ArrayList<CompositePath>> compositeIndexes = new ArrayList<ArrayList<CompositePath>>();

        //Simple
        ArrayList<CompositePath> compositeIndexSimple = new ArrayList<CompositePath>();
        CompositePath compositePath1 = new CompositePath();
        compositePath1.setPath("/" + NUMBER_FIELD);
        compositePath1.setOrder(CompositePathSortOrder.Ascending);

        CompositePath compositePath2 = new CompositePath();
        compositePath2.setPath("/" + STRING_FIELD);
        compositePath2.setOrder(CompositePathSortOrder.Descending);

        compositeIndexSimple.add(compositePath1);
        compositeIndexSimple.add(compositePath2);

        //Max Columns
        ArrayList<CompositePath> compositeIndexMaxColumns = new ArrayList<CompositePath>();
        CompositePath compositePath3 = new CompositePath();
        compositePath3.setPath("/" + NUMBER_FIELD);
        compositePath3.setOrder(CompositePathSortOrder.Descending);

        CompositePath compositePath4 = new CompositePath();
        compositePath4.setPath("/" + STRING_FIELD);
        compositePath4.setOrder(CompositePathSortOrder.Ascending);

        CompositePath compositePath5 = new CompositePath();
        compositePath5.setPath("/" + NUMBER_FIELD_2);
        compositePath5.setOrder(CompositePathSortOrder.Descending);

        CompositePath compositePath6 = new CompositePath();
        compositePath6.setPath("/" + STRING_FIELD_2);
        compositePath6.setOrder(CompositePathSortOrder.Ascending);

        compositeIndexMaxColumns.add(compositePath3);
        compositeIndexMaxColumns.add(compositePath4);
        compositeIndexMaxColumns.add(compositePath5);
        compositeIndexMaxColumns.add(compositePath6);

        //Primitive Values
        ArrayList<CompositePath> compositeIndexPrimitiveValues = new ArrayList<CompositePath>();
        CompositePath compositePath7 = new CompositePath();
        compositePath7.setPath("/" + NUMBER_FIELD);
        compositePath7.setOrder(CompositePathSortOrder.Descending);

        CompositePath compositePath8 = new CompositePath();
        compositePath8.setPath("/" + STRING_FIELD);
        compositePath8.setOrder(CompositePathSortOrder.Ascending);

        CompositePath compositePath9 = new CompositePath();
        compositePath9.setPath("/" + BOOL_FIELD);
        compositePath9.setOrder(CompositePathSortOrder.Descending);

        CompositePath compositePath10 = new CompositePath();
        compositePath10.setPath("/" + NULL_FIELD);
        compositePath10.setOrder(CompositePathSortOrder.Ascending);

        compositeIndexPrimitiveValues.add(compositePath7);
        compositeIndexPrimitiveValues.add(compositePath8);
        compositeIndexPrimitiveValues.add(compositePath9);
        compositeIndexPrimitiveValues.add(compositePath10);

        //Long Strings
        ArrayList<CompositePath> compositeIndexLongStrings = new ArrayList<CompositePath>();
        CompositePath compositePath11 = new CompositePath();
        compositePath11.setPath("/" + STRING_FIELD);

        CompositePath compositePath12 = new CompositePath();
        compositePath12.setPath("/" + SHORT_STRING_FIELD);

        CompositePath compositePath13 = new CompositePath();
        compositePath13.setPath("/" + MEDIUM_STRING_FIELD);

        CompositePath compositePath14 = new CompositePath();
        compositePath14.setPath("/" + LONG_STRING_FIELD);

        compositeIndexLongStrings.add(compositePath11);
        compositeIndexLongStrings.add(compositePath12);
        compositeIndexLongStrings.add(compositePath13);
        compositeIndexLongStrings.add(compositePath14);

        compositeIndexes.add(compositeIndexSimple);
        compositeIndexes.add(compositeIndexMaxColumns);
        compositeIndexes.add(compositeIndexPrimitiveValues);
        compositeIndexes.add(compositeIndexLongStrings);

        indexingPolicy.setCompositeIndexes(compositeIndexes);
        cosmosContainerSettings.setIndexingPolicy(indexingPolicy);

        return cosmosContainerSettings;
    }

    public static CosmosContainer createCollection(CosmosClient client, String dbId, CosmosContainerSettings collectionDefinition) {
        return client.getDatabase(dbId).createContainer(collectionDefinition).block().getContainer();
    }

    public static void deleteCollection(CosmosClient client, String dbId, String collectionId) {
        client.getDatabase(dbId).getContainer(collectionId).delete().block();
    }

    public static CosmosItem createDocument(CosmosContainer cosmosContainer, CosmosItemSettings item) {
        return cosmosContainer.createItem(item).block().getCosmosItem();
    }

    // TODO: respect concurrencyLevel;
    public Flux<CosmosItemResponse> bulkInsert(CosmosContainer cosmosContainer,
                                                             List<CosmosItemSettings> documentDefinitionList,
                                                             int concurrencyLevel) {
        CosmosItemSettings first = documentDefinitionList.remove(0);
        Flux<CosmosItemResponse> result = Flux.from(cosmosContainer.createItem(first));
        for (CosmosItemSettings docDef : documentDefinitionList) {
            result.concatWith(cosmosContainer.createItem(docDef));
        }

        return result;
    }

    public List<CosmosItemSettings> bulkInsertBlocking(CosmosContainer cosmosContainer,
                                             List<CosmosItemSettings> documentDefinitionList) {
        return bulkInsert(cosmosContainer, documentDefinitionList, DEFAULT_BULK_INSERT_CONCURRENCY_LEVEL)
                .parallel()
                .runOn(Schedulers.parallel())
                .map(CosmosItemResponse::getCosmosItemSettings)
                .sequential()
                .collectList()
                .block();
    }

    public static ConsistencyLevel getAccountDefaultConsistencyLevel(CosmosClient client) {
        return client.getDatabaseAccount().block().getConsistencyPolicy().getDefaultConsistencyLevel();
    }

    public static CosmosUser createUser(CosmosClient client, String databaseId, CosmosUserSettings userSettings) {
        return client.getDatabase(databaseId).read().block().getDatabase().createUser(userSettings).block().getUser();
    }

    public static CosmosUser safeCreateUser(CosmosClient client, String databaseId, CosmosUserSettings user) {
        deleteUserIfExists(client, databaseId, user.getId());
        return createUser(client, databaseId, user);
    }

    private static CosmosContainer safeCreateCollection(CosmosClient client, String databaseId, CosmosContainerSettings collection, CosmosContainerRequestOptions options) {
        deleteCollectionIfExists(client, databaseId, collection.getId());
        return createCollection(databaseId, collection, options);
    }

    static protected CosmosContainerSettings getCollectionDefinition() {
        PartitionKeyDefinition partitionKeyDef = new PartitionKeyDefinition();
        ArrayList<String> paths = new ArrayList<String>();
        paths.add("/mypk");
        partitionKeyDef.setPaths(paths);

        CosmosContainerSettings collectionDefinition = new CosmosContainerSettings(UUID.randomUUID().toString(), partitionKeyDef);

        return collectionDefinition;
    }

    static protected CosmosContainerSettings getCollectionDefinitionWithRangeRangeIndex() {
        PartitionKeyDefinition partitionKeyDef = new PartitionKeyDefinition();
        ArrayList<String> paths = new ArrayList<>();
        paths.add("/mypk");
        partitionKeyDef.setPaths(paths);
        IndexingPolicy indexingPolicy = new IndexingPolicy();
        Collection<IncludedPath> includedPaths = new ArrayList<>();
        IncludedPath includedPath = new IncludedPath();
        includedPath.setPath("/*");
        Collection<Index> indexes = new ArrayList<>();
        Index stringIndex = Index.Range(DataType.String);
        stringIndex.set("precision", -1);
        indexes.add(stringIndex);

        Index numberIndex = Index.Range(DataType.Number);
        numberIndex.set("precision", -1);
        indexes.add(numberIndex);
        includedPath.setIndexes(indexes);
        includedPaths.add(includedPath);
        indexingPolicy.setIncludedPaths(includedPaths);

        CosmosContainerSettings cosmosContainerSettings = new CosmosContainerSettings(UUID.randomUUID().toString(), partitionKeyDef);
        cosmosContainerSettings.setIndexingPolicy(indexingPolicy);

        return cosmosContainerSettings;
    }

    public static void deleteCollectionIfExists(CosmosClient client, String databaseId, String collectionId) {
        CosmosDatabase database = client.getDatabase(databaseId).read().block().getDatabase();
        List<CosmosContainerSettings> res = database.queryContainers(String.format("SELECT * FROM root r where r.id = '%s'", collectionId), null)
                .flatMap(page -> Flux.fromIterable(page.getResults()))
                .collectList()
                .block();
        
        if (!res.isEmpty()) {
            deleteCollection(database, collectionId);
        }
    }

    public static void deleteCollection(CosmosDatabase cosmosDatabase, String collectionId) {
        cosmosDatabase.getContainer(collectionId).delete().block();
    }

    public static void deleteCollection(CosmosContainer cosmosContainer) {
        cosmosContainer.delete().block();
    }

    public static void deleteDocumentIfExists(CosmosClient client, String databaseId, String collectionId, String docId) {
        FeedOptions options = new FeedOptions();
        options.setPartitionKey(new PartitionKey(docId));
        CosmosContainer cosmosContainer = client.getDatabase(databaseId).read().block().getDatabase().getContainer(collectionId).read().block().getContainer();
        List<CosmosItemSettings> res = cosmosContainer
                .queryItems(String.format("SELECT * FROM root r where r.id = '%s'", docId), options)
                .flatMap(page -> Flux.fromIterable(page.getResults()))
                .collectList().block();

        if (!res.isEmpty()) {
            deleteDocument(cosmosContainer, docId);
        }
    }

    public static void safeDeleteDocument(CosmosContainer cosmosContainer, String documentId, Object partitionKey) {
        if (cosmosContainer != null && documentId != null) {
            try {
                cosmosContainer.getItem(documentId, partitionKey).read().block().getCosmosItem().delete().block();
            } catch (Exception e) {
                DocumentClientException dce = com.microsoft.azure.cosmosdb.rx.internal.Utils.as(e, DocumentClientException.class);
                if (dce == null || dce.getStatusCode() != 404) {
                    throw e;
                }
            }
        }
    }

    public static void deleteDocument(CosmosContainer cosmosContainer, String documentId) {
        cosmosContainer.getItem(documentId, PartitionKey.None).read().block().getCosmosItem().delete();
    }

    public static void deleteUserIfExists(CosmosClient client, String databaseId, String userId) {
        CosmosDatabase database = client.getDatabase(databaseId).read().block().getDatabase();
        List<CosmosUserSettings> res = database
                .queryUsers(String.format("SELECT * FROM root r where r.id = '%s'", userId), null)
                .flatMap(page -> Flux.fromIterable(page.getResults()))
                .collectList().block();
        if (!res.isEmpty()) {
            deleteUser(database, userId);
        }
    }

    public static void deleteUser(CosmosDatabase database, String userId) {
        database.getUser(userId).read().block().getUser().delete(null).block();
    }

    static private CosmosDatabase safeCreateDatabase(CosmosClient client, CosmosDatabaseSettings databaseSettings) {
        safeDeleteDatabase(client.getDatabase(databaseSettings.getId()));
        return client.createDatabase(databaseSettings).block().getDatabase();
    }

    static protected CosmosDatabase createDatabase(CosmosClient client, String databaseId) {
        CosmosDatabaseSettings databaseSettings = new CosmosDatabaseSettings(databaseId);
        return client.createDatabase(databaseSettings).block().getDatabase();
    }

    static protected CosmosDatabase createDatabaseIfNotExists(CosmosClient client, String databaseId) {
        List<CosmosDatabaseSettings> res = client.queryDatabases(String.format("SELECT * FROM r where r.id = '%s'", databaseId), null)
                .flatMap(p -> Flux.fromIterable(p.getResults()))
                .collectList()
                .block();
        if (res.size() != 0) {
            return client.getDatabase(databaseId).read().block().getDatabase();
        } else {
            CosmosDatabaseSettings databaseSettings = new CosmosDatabaseSettings(databaseId);
            return client.createDatabase(databaseSettings).block().getDatabase();
        }
    }

    static protected void safeDeleteDatabase(CosmosDatabase database) {
        if (database != null) {
            try {
                database.delete().block();
            } catch (Exception e) {
            }
        }
    }

    static protected void safeDeleteAllCollections(CosmosDatabase database) {
        if (database != null) {
            List<CosmosContainerSettings> collections = database.listContainers()
                    .flatMap(p -> Flux.fromIterable(p.getResults()))
                    .collectList()
                    .block();

            for(CosmosContainerSettings collection: collections) {
                database.getContainer(collection.getId()).read().block().getContainer().delete().block();
            }
        }
    }

    static protected void safeDeleteCollection(CosmosContainer collection) {
        if (collection != null) {
            try {
                collection.delete().block();
            } catch (Exception e) {
            }
        }
    }

    static protected void safeDeleteCollection(CosmosDatabase database, String collectionId) {
        if (database != null && collectionId != null) {
            try {
                database.getContainer(collectionId).read().block().getContainer().delete().block();
            } catch (Exception e) {
            }
        }
    }

    static protected void safeClose(CosmosClient client) {
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public <T extends CosmosResponse> void validateSuccess(Mono<T> single, CosmosResponseValidator<T> validator)
            throws InterruptedException {
        validateSuccess(single.flux(), validator, subscriberValidationTimeout);
    }

    public static <T extends CosmosResponse> void validateSuccess(Flux<T> flowable,
            CosmosResponseValidator<T> validator, long timeout) throws InterruptedException {

        TestSubscriber<T> testSubscriber = new TestSubscriber<>();

        flowable.subscribe(testSubscriber);
        testSubscriber.awaitTerminalEvent(timeout, TimeUnit.MILLISECONDS);
        testSubscriber.assertNoErrors();
        testSubscriber.assertComplete();
        testSubscriber.assertValueCount(1);
        validator.validate(testSubscriber.values().get(0));
    }

    public <T extends Resource, U extends CosmosResponse> void validateFailure(Mono<U> mono, FailureValidator validator)
            throws InterruptedException {
        validateFailure(mono.flux(), validator, subscriberValidationTimeout);
    }

    public static <T extends Resource, U extends CosmosResponse> void validateFailure(Flux<U> flowable,
            FailureValidator validator, long timeout) throws InterruptedException {

        TestSubscriber<CosmosResponse> testSubscriber = new TestSubscriber<>();

        flowable.subscribe(testSubscriber);
        testSubscriber.awaitTerminalEvent(timeout, TimeUnit.MILLISECONDS);
        testSubscriber.assertNotComplete();
        testSubscriber.assertTerminated();
        assertThat(testSubscriber.errors()).hasSize(1);
        validator.validate((Throwable) testSubscriber.getEvents().get(1).get(0));
    }

    public <T extends Resource> void validateQuerySuccess(Flux<FeedResponse<T>> flowable,
            FeedResponseListValidator<T> validator) {
        validateQuerySuccess(flowable, validator, subscriberValidationTimeout);
    }

    public static <T extends Resource> void validateQuerySuccess(Flux<FeedResponse<T>> flowable,
            FeedResponseListValidator<T> validator, long timeout) {

        TestSubscriber<FeedResponse<T>> testSubscriber = new TestSubscriber<>();

        flowable.subscribe(testSubscriber);
        testSubscriber.awaitTerminalEvent(timeout, TimeUnit.MILLISECONDS);
        testSubscriber.assertNoErrors();
        testSubscriber.assertComplete();
        validator.validate(testSubscriber.getEvents().get(0).stream().map(object -> (FeedResponse<T>) object)
                .collect(Collectors.toList()));
    }

    public <T extends Resource> void validateQueryFailure(Flux<FeedResponse<T>> flowable, FailureValidator validator) {
        validateQueryFailure(flowable, validator, subscriberValidationTimeout);
    }

    public static <T extends Resource> void validateQueryFailure(Flux<FeedResponse<T>> flowable,
            FailureValidator validator, long timeout) {

        TestSubscriber<FeedResponse<T>> testSubscriber = new TestSubscriber<>();

        flowable.subscribe(testSubscriber);
        testSubscriber.awaitTerminalEvent(timeout, TimeUnit.MILLISECONDS);
        testSubscriber.assertNotComplete();
        testSubscriber.assertTerminated();
        assertThat(testSubscriber.getEvents().get(1)).hasSize(1);
        validator.validate((Throwable) testSubscriber.getEvents().get(1).get(0));
    }

    @DataProvider
    public static Object[][] clientBuilders() {
        return new Object[][] { { createGatewayRxDocumentClient(ConsistencyLevel.Session, false, null) } };
    }

    @DataProvider
    public static Object[][] clientBuildersWithSessionConsistency() {
        return new Object[][] {
                { createGatewayRxDocumentClient(ConsistencyLevel.Session, false, null) },
                { createDirectRxDocumentClient(ConsistencyLevel.Session, Protocol.Https, false, null) },
                { createDirectRxDocumentClient(ConsistencyLevel.Session, Protocol.Tcp, false, null) }
        };
    }

    private static ConsistencyLevel parseConsistency(String consistency) {
        if (consistency != null) {
            for (ConsistencyLevel consistencyLevel : ConsistencyLevel.values()) {
                if (consistencyLevel.name().toLowerCase().equals(consistency.toLowerCase())) {
                    return consistencyLevel;
                }
            }
        }

        logger.error("Invalid configured test consistency [{}].", consistency);
        throw new IllegalStateException("Invalid configured test consistency " + consistency);
    }

    private static List<String> parsePreferredLocation(String preferredLocations) {
        if (StringUtils.isEmpty(preferredLocations)) {
            return null;
        }

        try {
            return objectMapper.readValue(preferredLocations, new TypeReference<List<String>>(){});
        } catch (Exception e) {
            logger.error("Invalid configured test preferredLocations [{}].", preferredLocations);
            throw new IllegalStateException("Invalid configured test preferredLocations " + preferredLocations);
        }
    }

    @DataProvider
    public static Object[][] simpleClientBuildersWithDirect() {
        return simpleClientBuildersWithDirect(Protocol.Https, Protocol.Tcp);
    }

    @DataProvider
    public static Object[][] simpleClientBuildersWithDirectHttps() {
        return simpleClientBuildersWithDirect(Protocol.Https);
    }

    @DataProvider
    public static Object[][] simpleClientBuildersWithDirectTcp() {
        return simpleClientBuildersWithDirect(Protocol.Tcp);
    }

    private static Object[][] simpleClientBuildersWithDirect(Protocol... protocols) {

        accountConsistency = parseConsistency(TestConfigurations.CONSISTENCY);
        logger.info("Max test consistency to use is [{}]", accountConsistency);
        List<ConsistencyLevel> testConsistencies = new ArrayList<>();

        switch (accountConsistency) {
            case Strong:
            case BoundedStaleness:
            case Session:
            case ConsistentPrefix:
            case Eventual:
                testConsistencies.add(ConsistencyLevel.Eventual);
                break;
            default:
                throw new IllegalStateException("Invalid configured test consistency " + accountConsistency);
        }

        List<String> preferredLocation = parsePreferredLocation(TestConfigurations.PREFERRED_LOCATIONS);
        boolean isMultiMasterEnabled = preferredLocation != null && accountConsistency == ConsistencyLevel.Session;

        List<Builder> cosmosConfigurations = new ArrayList<>();
        cosmosConfigurations.add(createGatewayRxDocumentClient(ConsistencyLevel.Session, false, null));

        for (Protocol protocol : protocols) {
            testConsistencies.forEach(consistencyLevel -> cosmosConfigurations.add(createDirectRxDocumentClient(consistencyLevel,
                                                                                                    protocol,
                                                                                                    isMultiMasterEnabled,
                                                                                                    preferredLocation)));
        }

        cosmosConfigurations.forEach(c -> logger.info("Will Use ConnectionMode [{}], Consistency [{}], Protocol [{}]",
                                          c.getConnectionPolicy().getConnectionMode(),
                                          c.getDesiredConsistencyLevel(),
                                          c.getConfigs().getProtocol()
        ));

        return cosmosConfigurations.stream().map(b -> new Object[]{b}).collect(Collectors.toList()).toArray(new Object[0][]);
    }

    @DataProvider
    public static Object[][] clientBuildersWithDirect() {
        return clientBuildersWithDirectAllConsistencies(Protocol.Https, Protocol.Tcp);
    }

    @DataProvider
    public static Object[][] clientBuildersWithDirectHttps() {
        return clientBuildersWithDirectAllConsistencies(Protocol.Https);
    }

    @DataProvider
    public static Object[][] clientBuildersWithDirectTcp() {
        return clientBuildersWithDirectAllConsistencies(Protocol.Tcp);
    }

    @DataProvider
    public static Object[][] clientBuildersWithDirectSession() {
        return clientBuildersWithDirectSession(Protocol.Https, Protocol.Tcp);
    }
    
    private static Object[][] clientBuildersWithDirectSession(Protocol... protocols) {
        return clientBuildersWithDirect(new ArrayList<ConsistencyLevel>(){{add(ConsistencyLevel.Session);}} , protocols);
    }

    private static Object[][] clientBuildersWithDirectAllConsistencies(Protocol... protocols) {
        accountConsistency = parseConsistency(TestConfigurations.CONSISTENCY);
        logger.info("Max test consistency to use is [{}]", accountConsistency);
        List<ConsistencyLevel> testConsistencies = new ArrayList<>();

        /*
        switch (accountConsistency) {
        
            case Strong:
                testConsistencies.add(ConsistencyLevel.Strong);
            case BoundedStaleness:
                testConsistencies.add(ConsistencyLevel.BoundedStaleness);
            case Session:
                testConsistencies.add(ConsistencyLevel.Session);
            case ConsistentPrefix:
                testConsistencies.add(ConsistencyLevel.ConsistentPrefix);
            case Eventual:
                testConsistencies.add(ConsistencyLevel.Eventual);
                break;
            default:
                throw new IllegalStateException("Invalid configured test consistency " + accountConsistency);
        }
        */
        testConsistencies.add(ConsistencyLevel.Session);
        
        return clientBuildersWithDirect(testConsistencies, protocols);
    }
    
    private static Object[][] clientBuildersWithDirect(List<ConsistencyLevel> testConsistencies, Protocol... protocols) {

        List<String> preferredLocation = parsePreferredLocation(TestConfigurations.PREFERRED_LOCATIONS);
        boolean isMultiMasterEnabled = preferredLocation != null && accountConsistency == ConsistencyLevel.Session;

        List<Builder> cosmosConfigurations = new ArrayList<>();
        cosmosConfigurations.add(createGatewayRxDocumentClient(ConsistencyLevel.Session, isMultiMasterEnabled, preferredLocation));

        for (Protocol protocol : protocols) {
            testConsistencies.forEach(consistencyLevel -> cosmosConfigurations.add(createDirectRxDocumentClient(consistencyLevel,
                                                                                                    protocol,
                                                                                                    isMultiMasterEnabled,
                                                                                                    preferredLocation)));
        }

        cosmosConfigurations.forEach(c -> logger.info("Will Use ConnectionMode [{}], Consistency [{}], Protocol [{}]",
                                          c.getConnectionPolicy().getConnectionMode(),
                                          c.getDesiredConsistencyLevel(),
                                          c.getConfigs().getProtocol()
        ));

        return cosmosConfigurations.stream().map(c -> new Object[]{c}).collect(Collectors.toList()).toArray(new Object[0][]);
    }

    static protected Builder createGatewayHouseKeepingDocumentClient() {
        ConnectionPolicy connectionPolicy = new ConnectionPolicy();
        connectionPolicy.setConnectionMode(ConnectionMode.Gateway);
        RetryOptions options = new RetryOptions();
        options.setMaxRetryWaitTimeInSeconds(SUITE_SETUP_TIMEOUT);
        connectionPolicy.setRetryOptions(options);
        return new Builder().endpoint(TestConfigurations.HOST)
                .key(TestConfigurations.MASTER_KEY)
                .connectionPolicy(connectionPolicy)
                .consistencyLevel(ConsistencyLevel.Session);
    }

    static protected Builder createGatewayRxDocumentClient(ConsistencyLevel consistencyLevel, boolean multiMasterEnabled, List<String> preferredLocations) {
        ConnectionPolicy connectionPolicy = new ConnectionPolicy();
        connectionPolicy.setConnectionMode(ConnectionMode.Gateway);
        connectionPolicy.setUsingMultipleWriteLocations(multiMasterEnabled);
        connectionPolicy.setPreferredLocations(preferredLocations);
        return new Builder().endpoint(TestConfigurations.HOST)
                .key(TestConfigurations.MASTER_KEY)
                .connectionPolicy(connectionPolicy)
                .consistencyLevel(consistencyLevel);
    }

    static protected Builder createGatewayRxDocumentClient() {
        return createGatewayRxDocumentClient(ConsistencyLevel.Session, false, null);
    }

    static protected Builder createDirectRxDocumentClient(ConsistencyLevel consistencyLevel,
                                                                              Protocol protocol,
                                                                              boolean multiMasterEnabled,
                                                                              List<String> preferredLocations) {
        ConnectionPolicy connectionPolicy = new ConnectionPolicy();
        connectionPolicy.setConnectionMode(ConnectionMode.Direct);

        if (preferredLocations != null) {
            connectionPolicy.setPreferredLocations(preferredLocations);
        }

        if (multiMasterEnabled && consistencyLevel == ConsistencyLevel.Session) {
            connectionPolicy.setUsingMultipleWriteLocations(true);
        }

        Configs configs = spy(new Configs());
        doAnswer((Answer<Protocol>)invocation -> protocol).when(configs).getProtocol();

        return new Builder().endpoint(TestConfigurations.HOST)
                .key(TestConfigurations.MASTER_KEY)
                .connectionPolicy(connectionPolicy)
                .consistencyLevel(consistencyLevel)
                .configs(configs);
    }

    protected int expectedNumberOfPages(int totalExpectedResult, int maxPageSize) {
        return Math.max((totalExpectedResult + maxPageSize - 1 ) / maxPageSize, 1);
    }

    @DataProvider(name = "queryMetricsArgProvider")
    public Object[][] queryMetricsArgProvider() {
        return new Object[][]{
                {true},
                {false},
        };
    }

    /*
    public static class VerboseTestSubscriber<T> extends TestSubscriber<T> {
        @Override
        public void assertNoErrors() {
            List<Throwable> onErrorEvents = getOnErrorEvents();
            StringBuilder errorMessageBuilder = new StringBuilder();
            if (!onErrorEvents.isEmpty()) {
                for(Throwable throwable : onErrorEvents) {
                    StringWriter sw = new StringWriter();
                    PrintWriter pw = new PrintWriter(sw);
                    throwable.printStackTrace(pw);
                    String sStackTrace = sw.toString(); // stack trace as a string
                    errorMessageBuilder.append(sStackTrace);
                }

                AssertionError ae = new AssertionError(errorMessageBuilder.toString());
                throw ae;
            }
        }
    }
    */
}
