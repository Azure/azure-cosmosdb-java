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
package com.azure.data.cosmos.rx;

import com.azure.data.cosmos.internal.changefeed.ChangeFeedObserver;
import com.azure.data.cosmos.internal.changefeed.ChangeFeedObserverCloseReason;
import com.azure.data.cosmos.internal.changefeed.ChangeFeedObserverContext;
import com.azure.data.cosmos.ChangeFeedProcessor;
import com.azure.data.cosmos.ChangeFeedProcessorOptions;
import com.azure.data.cosmos.CosmosClient;
import com.azure.data.cosmos.CosmosClientBuilder;
import com.azure.data.cosmos.CosmosContainer;
import com.azure.data.cosmos.CosmosContainerRequestOptions;
import com.azure.data.cosmos.CosmosContainerSettings;
import com.azure.data.cosmos.CosmosDatabase;
import com.azure.data.cosmos.CosmosDatabaseForTest;
import com.azure.data.cosmos.CosmosItemProperties;
import com.azure.data.cosmos.SerializationFormattingPolicy;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

public class ChangeFeedProcessorTest extends TestSuiteBase {
    private final static Logger log = LoggerFactory.getLogger(ChangeFeedProcessorTest.class);

    private CosmosDatabase createdDatabase;
    private CosmosContainer createdFeedCollection;
    private CosmosContainer createdLeaseCollection;
    private List<CosmosItemProperties> createdDocuments;
    private static Map<String, CosmosItemProperties> receivedDocuments;
    private static Map<String, String> activeLeases;
//    private final String databaseId = "testdb1";
    private final String databaseId = CosmosDatabaseForTest.generateId();
//    private final String hostName = "TestHost1";
    private final String hostName = RandomStringUtils.randomAlphabetic(6);

    private CosmosClient client;

    private ChangeFeedProcessor changeFeedProcessor;

    @Factory(dataProvider = "clientBuilders")
    public ChangeFeedProcessorTest(CosmosClientBuilder clientBuilder) {
        super(clientBuilder);
    }

    @Test(groups = { "emulator" }, timeOut = 60000)
    public void readFeedDocumentsStartFromBeginning() {

        Mono<ChangeFeedProcessor> changeFeedProcessorObservable = ChangeFeedProcessor.Builder()
            .hostName(hostName)
            .syncHandleChanges(docs -> {
                ChangeFeedProcessorTest.log.info("START processing from thread {}", Thread.currentThread().getId());
                for (CosmosItemProperties item : docs) {
                    processItem(item);
                }
                ChangeFeedProcessorTest.log.info("END processing from thread {}", Thread.currentThread().getId());
            })
            .feedContainerClient(createdFeedCollection)
            .leaseContainerClient(createdLeaseCollection)
            .options(new ChangeFeedProcessorOptions()
                .leaseRenewInterval(Duration.ofSeconds(20))
                .leaseAcquireInterval(Duration.ofSeconds(10))
                .leaseExpirationInterval(Duration.ofSeconds(30))
                .feedPollDelay(Duration.ofSeconds(2))
                .leasePrefix("TEST")
                .maxItemCount(10)
                .startFromBeginning(true)
                .minPartitionCount(1)
                .maxPartitionCount(3)
                .discardExistingLeases(true)
                .queryPartitionsMaxBatchSize(2)
                .degreeOfParallelism(1)
            )
            .build();

        try {
            changeFeedProcessorObservable.subscribeOn(Schedulers.elastic())
                .flatMap(processor -> {
                    changeFeedProcessor = processor;
                    return changeFeedProcessor.start().subscribeOn(Schedulers.elastic());
                }).timeout(Duration.ofSeconds(5)).subscribe();
        } catch (Exception ex) {
            log.error("Change feed processor did not start in the expected time", ex);
        }

        // Wait for the feed processor to receive and process the documents.
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        changeFeedProcessor.stop().subscribeOn(Schedulers.elastic()).timeout(Duration.ofSeconds(10)).subscribe();

        for (CosmosItemProperties item : createdDocuments) {
            assertThat(receivedDocuments.containsKey(item.id())).as("Document with id: " + item.id()).isTrue();
        }

     }

    @BeforeClass(groups = { "emulator" }, timeOut = SETUP_TIMEOUT, alwaysRun = true)
    public void beforeClass() {
        receivedDocuments = new ConcurrentHashMap<>();
        activeLeases = new ConcurrentHashMap<>();
        client = clientBuilder().build();

        try {
            client.getDatabase(databaseId).delete().block();
            Thread.sleep(500);
        } catch (Exception e){ }

//        try {
//            client.listDatabases()
//                .flatMap(cosmosDatabaseSettingsFeedResponse -> Flux.fromIterable(cosmosDatabaseSettingsFeedResponse.results()))
//                .flatMap(cosmosDatabaseSettings -> {
//                    CosmosDatabase cosmosDatabase = client.getDatabase(cosmosDatabaseSettings.id());
//                    return cosmosDatabase.delete();
//                }).blockLast();
//            Thread.sleep(500);
//        } catch (Exception e){ }

        createdDatabase = createDatabase(client, databaseId);

        CosmosContainerRequestOptions options = new CosmosContainerRequestOptions();
        options.offerThroughput(10100);
        createdFeedCollection = createCollection(createdDatabase, getCollectionDefinition(), options);

        createdLeaseCollection = createLeaseCollection();

        List<CosmosItemProperties> docDefList = new ArrayList<>();

        for(int i = 0; i < 10; i++) {
            docDefList.add(getDocumentDefinition());
        }

        createdDocuments = bulkInsertBlocking(createdFeedCollection, docDefList);
        waitIfNeededForReplicasToCatchUp(clientBuilder());
    }

    @AfterClass(groups = { "emulator" }, timeOut = SHUTDOWN_TIMEOUT, alwaysRun = true)
    public void afterClass() {
        safeDeleteDatabase(createdDatabase);

        // Allow some time for the collections and the database to be deleted before exiting.
        try {
            Thread.sleep(500);
        } catch (Exception e){ }
    }

    private CosmosItemProperties getDocumentDefinition() {
        String uuid = UUID.randomUUID().toString();
        CosmosItemProperties doc = new CosmosItemProperties(String.format("{ "
                + "\"id\": \"%s\", "
                + "\"mypk\": \"%s\", "
                + "\"sgmts\": [[6519456, 1471916863], [2498434, 1455671440]]"
                + "}"
            , uuid, uuid));
        return doc;
    }

    private CosmosContainer createLeaseCollection() {
        CosmosContainerRequestOptions options = new CosmosContainerRequestOptions();
        options.offerThroughput(400);
        CosmosContainerSettings collectionDefinition = new CosmosContainerSettings(UUID.randomUUID().toString(), "/id");
        return createCollection(createdDatabase, collectionDefinition, options);
    }

    private static synchronized void processItem(CosmosItemProperties item) {
        ChangeFeedProcessorTest.log.info("RECEIVED {}", item.toJson(SerializationFormattingPolicy.INDENTED));
        receivedDocuments.put(item.id(), item);
    }

    public static class TestChangeFeedObserverImpl implements ChangeFeedObserver {

        public TestChangeFeedObserverImpl() {}

        @Override
        public void open(ChangeFeedObserverContext context) {
            ChangeFeedProcessorTest.log.info("OPEN processing from thread {}", Thread.currentThread().getId());
            ChangeFeedProcessorTest.activeLeases.put(Thread.currentThread().getName(), context.getPartitionKeyRangeId());
        }

        @Override
        public void close(ChangeFeedObserverContext context, ChangeFeedObserverCloseReason reason) {
            ChangeFeedProcessorTest.log.info("CLOSE processing from thread {}", Thread.currentThread().getId());
            ChangeFeedProcessorTest.activeLeases.remove(Thread.currentThread().getName());
        }

        @Override
        public void processChanges(ChangeFeedObserverContext context, List<CosmosItemProperties> docs) {
            ChangeFeedProcessorTest.log.info("START processing from thread {}", Thread.currentThread().getId());
            for (CosmosItemProperties item : docs) {
                processItem(item);
            }
            ChangeFeedProcessorTest.log.info("END processing from thread {}", Thread.currentThread().getId());
        }
    }
}
