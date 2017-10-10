/**
 * The MIT License (MIT)
 * Copyright (c) 2017 Microsoft Corporation
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
package com.microsoft.azure.documentdb.rx;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.testng.annotations.DataProvider;

import com.microsoft.azure.documentdb.ConnectionMode;
import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.FeedOptions;
import com.microsoft.azure.documentdb.FeedResponsePage;
import com.microsoft.azure.documentdb.PartitionKey;
import com.microsoft.azure.documentdb.PartitionKeyDefinition;
import com.microsoft.azure.documentdb.RequestOptions;
import com.microsoft.azure.documentdb.Resource;
import com.microsoft.azure.documentdb.ResourceResponse;
import com.microsoft.azure.documentdb.User;
import com.microsoft.azure.documentdb.internal.directconnectivity.HttpClientFactory;
import com.microsoft.azure.documentdb.rx.internal.RxWrapperDocumentClientImpl;

import rx.Observable;
import rx.observers.TestSubscriber;

public class TestSuiteBase {

    protected static final int TIMEOUT = 8000;
    protected static final int SETUP_TIMEOUT = 12000;
    protected static final int SHUTDOWN_TIMEOUT = 12000;

    protected int subscriberValidationTimeout = TIMEOUT;

    static {
        HttpClientFactory.DISABLE_HOST_NAME_VERIFICATION = true;
    }

    public static String getDatabaseId(Class<?> klass) {
        return String.format("java.rx.%s", klass.getName());
    }

    public static DocumentCollection createCollection(AsyncDocumentClient client, String databaseId,
            DocumentCollection collection) {
        return client.createCollection("dbs/" + databaseId, collection, null).toBlocking().single().getResource();
    }

    public static Document createDocument(AsyncDocumentClient client, String databaseId, String collectionId, Document document) {
        return client.createDocument(Utils.getCollectionNameLink(databaseId, collectionId), document, null, false).toBlocking().single().getResource();
    }

    public static User createUser(AsyncDocumentClient client, String databaseId, User user) {
        return client.createUser("dbs/" + databaseId, user, null).toBlocking().single().getResource();
    }
    
    public static DocumentCollection safeCreateCollection(AsyncDocumentClient client, String databaseId,
            DocumentCollection collection) {
        deleteCollectionIfExists(client, databaseId, collection.getId());
        return createCollection(client, databaseId, collection);
    }

    public static Document safeCreateDocument(AsyncDocumentClient client, String databaseId, String collectionId, Document document) {
        deleteDocumentIfExists(client, databaseId, collectionId, document.getId());
        return createDocument(client, databaseId, collectionId, document);
    }

    public static User safeCreateUser(AsyncDocumentClient client, String databaseId, User user) {
        deleteUserIfExists(client, databaseId, user.getId());
        return createUser(client, databaseId, user);
    }

    public static String getCollectionLink(DocumentCollection collection) {
        return collection.getSelfLink();
    }

    static protected DocumentCollection getCollectionDefinition() {
        PartitionKeyDefinition partitionKeyDef = new PartitionKeyDefinition();
        ArrayList<String> paths = new ArrayList<String>();
        paths.add("/mypk");
        partitionKeyDef.setPaths(paths);

        RequestOptions options = new RequestOptions();
        options.setOfferThroughput(10100);
        DocumentCollection collectionDefinition = new DocumentCollection();
        collectionDefinition.setId(UUID.randomUUID().toString());
        collectionDefinition.setPartitionKey(partitionKeyDef);

        return collectionDefinition;
    }

    public static void deleteCollectionIfExists(AsyncDocumentClient client, String databaseId, String collectionId) {
        List<DocumentCollection> res = client.queryCollections("dbs/" + databaseId,
                String.format("SELECT * FROM root r where r.id = '%s'", collectionId), null).toBlocking().single()
                .getResults();
        if (!res.isEmpty()) {
            deleteCollection(client, Utils.getCollectionNameLink(databaseId, collectionId));
        }
    }

    public static void deleteCollection(AsyncDocumentClient client, String collectionLink) {
        client.deleteCollection(collectionLink, null).toBlocking().single();
    }

    public static void deleteDocumentIfExists(AsyncDocumentClient client, String databaseId, String collectionId, String docId) {
        FeedOptions options = new FeedOptions();
        options.setPartitionKey(new PartitionKey(docId));
        List<Document> res = client
                .queryDocuments(Utils.getCollectionNameLink(databaseId, collectionId), String.format("SELECT * FROM root r where r.id = '%s'", docId), options)
                .toBlocking().single().getResults();
        if (!res.isEmpty()) {
            deleteDocument(client, Utils.getDocumentNameLink(databaseId, collectionId, docId));
        }
    }

    public static void deleteDocument(AsyncDocumentClient client, String documentLink) {
        client.deleteDocument(documentLink, null).toBlocking().single();
    }

    public static void deleteUserIfExists(AsyncDocumentClient client, String databaseId, String userId) {
        List<User> res = client
                .queryUsers("dbs/" + databaseId, String.format("SELECT * FROM root r where r.id = '%s'", userId), null)
                .toBlocking().single().getResults();
        if (!res.isEmpty()) {
            deleteUser(client, Utils.getUserNameLink(databaseId, userId));
        }
    }

    public static void deleteUser(AsyncDocumentClient client, String userLink) {
        client.deleteUser(userLink, null).toBlocking().single();
    }

    public static String getDatabaseLink(Database database) {
        return database.getSelfLink();
    }

    static protected Database safeCreateDatabase(AsyncDocumentClient client, Database database) {
        try {
            deleteDatabase(client, database.getId());
        } catch (Exception e) {
        }
        return createDatabase(client, database);
    }

    static private Database createDatabase(AsyncDocumentClient client, Database database) {
        Observable<ResourceResponse<Database>> databaseObservable = client.createDatabase(database, null);
        return databaseObservable.toBlocking().single().getResource();
    }

    static protected Database createDatabase(AsyncDocumentClient client, String databaseId) {
        Database databaseDefinition = new Database();
        databaseDefinition.setId(databaseId);
        return createDatabase(client, databaseDefinition);
    }

    static protected void deleteDatabase(AsyncDocumentClient client, String databaseId) {
        client.deleteDatabase(Utils.getDatabaseNameLink(databaseId), null).toBlocking().single();
    }

    public <T extends Resource> void validateSuccess(Observable<ResourceResponse<T>> observable,
            ResourceResponseValidator<T> validator) throws InterruptedException {
        validateSuccess(observable, validator, subscriberValidationTimeout);
    }

    public static <T extends Resource> void validateSuccess(Observable<ResourceResponse<T>> observable,
            ResourceResponseValidator<T> validator, long timeout) throws InterruptedException {

        TestSubscriber<ResourceResponse<T>> testSubscriber = new TestSubscriber<ResourceResponse<T>>();

        observable.subscribe(testSubscriber);
        testSubscriber.awaitTerminalEvent(timeout, TimeUnit.MILLISECONDS);
        testSubscriber.assertNoErrors();
        testSubscriber.assertCompleted();
        testSubscriber.assertValueCount(1);
        validator.validate(testSubscriber.getOnNextEvents().get(0));
    }

    public <T extends Resource> void validateFailure(Observable<ResourceResponse<T>> observable,
            FailureValidator validator) throws InterruptedException {
        validateFailure(observable, validator, subscriberValidationTimeout);
    }

    public static <T extends Resource> void validateFailure(Observable<ResourceResponse<T>> observable,
            FailureValidator validator, long timeout) throws InterruptedException {

        TestSubscriber<ResourceResponse<T>> testSubscriber = new TestSubscriber<>();

        observable.subscribe(testSubscriber);
        testSubscriber.awaitTerminalEvent(timeout, TimeUnit.MILLISECONDS);
        testSubscriber.assertNotCompleted();
        testSubscriber.assertTerminalEvent();
        assertThat(testSubscriber.getOnErrorEvents()).hasSize(1);
        validator.validate(testSubscriber.getOnErrorEvents().get(0));
    }

    public <T extends Resource> void validateQuerySuccess(Observable<FeedResponsePage<T>> observable,
            FeedResponsePageListValidator<T> validator) throws InterruptedException {
        validateQuerySuccess(observable, validator, subscriberValidationTimeout);
    }

    public static <T extends Resource> void validateQuerySuccess(Observable<FeedResponsePage<T>> observable,
            FeedResponsePageListValidator<T> validator, long timeout) throws InterruptedException {

        TestSubscriber<FeedResponsePage<T>> testSubscriber = new TestSubscriber<>();

        observable.subscribe(testSubscriber);
        testSubscriber.awaitTerminalEvent(timeout, TimeUnit.MILLISECONDS);
        testSubscriber.assertNoErrors();
        testSubscriber.assertCompleted();
        validator.validate(testSubscriber.getOnNextEvents());
    }

    public <T extends Resource> void validateQueryFailure(Observable<FeedResponsePage<T>> observable,
            FailureValidator validator) throws InterruptedException {
        validateQueryFailure(observable, validator, subscriberValidationTimeout);
    }

    public static <T extends Resource> void validateQueryFailure(Observable<FeedResponsePage<T>> observable,
            FailureValidator validator, long timeout) throws InterruptedException {

        TestSubscriber<FeedResponsePage<T>> testSubscriber = new TestSubscriber<>();

        observable.subscribe(testSubscriber);
        testSubscriber.awaitTerminalEvent(timeout, TimeUnit.MILLISECONDS);
        testSubscriber.assertNotCompleted();
        testSubscriber.assertTerminalEvent();
        assertThat(testSubscriber.getOnErrorEvents()).hasSize(1);
        validator.validate(testSubscriber.getOnErrorEvents().get(0));
    }

    @DataProvider
    public static Object[][] clientBuilders() {
        return new Object[][] { { createGatewayRxDocumentClient() }, { createDirectHttpsRxDocumentClient() } };
    }

    static protected AsyncDocumentClient.Builder createGatewayRxDocumentClient() {
        ConnectionPolicy connectionPolicy = new ConnectionPolicy();
        connectionPolicy.setConnectionMode(ConnectionMode.Gateway);
        return new AsyncDocumentClient.Builder().withServiceEndpoint(TestConfigurations.HOST)
                .withMasterKey(TestConfigurations.MASTER_KEY).withConnectionPolicy(connectionPolicy)
                .withConsistencyLevel(ConsistencyLevel.Session);
    }

    static protected AsyncDocumentClient.Builder createDirectHttpsRxDocumentClient() {
        ConnectionPolicy connectionPolicy = new ConnectionPolicy();
        connectionPolicy.setConnectionMode(ConnectionMode.DirectHttps);
        return new AsyncDocumentClient.Builder().withServiceEndpoint(TestConfigurations.HOST)
                .withMasterKey(TestConfigurations.MASTER_KEY).withConnectionPolicy(connectionPolicy)
                .withConsistencyLevel(ConsistencyLevel.Session);
    }

    static protected AsyncDocumentClient.Builder createRxWrapperDocumentClient() {

        return new AsyncDocumentClient.Builder() {
            /*
             * (non-Javadoc)
             * 
             * @see com.microsoft.azure.documentdb.rx.AsyncDocumentClient.Builder#build()
             */
            @Override
            public AsyncDocumentClient build() {
                return new RxWrapperDocumentClientImpl(new DocumentClient(TestConfigurations.HOST,
                        TestConfigurations.MASTER_KEY, ConnectionPolicy.GetDefault(), ConsistencyLevel.Session));
            }
        };
    }
}
