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

import com.microsoft.azure.cosmosdb.Database;
import com.microsoft.azure.cosmosdb.Document;
import com.microsoft.azure.cosmosdb.DocumentClientException;
import com.microsoft.azure.cosmosdb.DocumentCollection;
import com.microsoft.azure.cosmosdb.PartitionKey;
import com.microsoft.azure.cosmosdb.RequestOptions;
import com.microsoft.azure.cosmosdb.ResourceResponse;
import com.microsoft.azure.cosmosdb.Undefined;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.Protocol;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient.Builder;
import org.apache.commons.lang3.StringUtils;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;
import rx.Observable;

import java.util.Date;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.commons.io.FileUtils.ONE_MB;
import static org.assertj.core.api.Assertions.assertThat;

public class DocumentCrudTest extends TestSuiteBase {

    private Database createdDatabase;
    private DocumentCollection createdCollection;

    private AsyncDocumentClient client;
    
    @Factory(dataProvider = "clientBuildersWithDirect")
    public DocumentCrudTest(Builder clientBuilder) {
        this.clientBuilder = clientBuilder;
    }

    @Test(groups = { "simple" }, timeOut = TIMEOUT)
    public void createDocument() {
        Document docDefinition = getDocumentDefinition();

        Observable<ResourceResponse<Document>> createObservable = client
                .createDocument(getCollectionLink(), docDefinition, null, false);

        ResourceResponseValidator<Document> validator = new ResourceResponseValidator.Builder<Document>()
                .withId(docDefinition.getId())
                .build();

        validateSuccess(createObservable, validator);
    }

    @Test(groups = { "simple" }, timeOut = TIMEOUT)
    public void createLargeDocument() {
        Document docDefinition = getDocumentDefinition();

        //Keep size as ~ 1.5MB to account for size of other props
        int size = (int) (ONE_MB * 1.5);
        docDefinition.set("largeString", StringUtils.repeat("x", size));

        Observable<ResourceResponse<Document>> createObservable = client
                .createDocument(getCollectionLink(), docDefinition, null, false);

        ResourceResponseValidator<Document> validator = new ResourceResponseValidator.Builder<Document>()
                .withId(docDefinition.getId())
                .build();

        validateSuccess(createObservable, validator);
    }

    @Test(groups = { "simple" }, timeOut = TIMEOUT)
    public void createDocumentWithVeryLargePartitionKey() {
        Document docDefinition = getDocumentDefinition();
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < 100; i++) {
            sb.append(i).append("x");
        }
        docDefinition.set("mypk", sb.toString());

        Observable<ResourceResponse<Document>> createObservable = client
                .createDocument(getCollectionLink(), docDefinition, null, false);

        ResourceResponseValidator<Document> validator = new ResourceResponseValidator.Builder<Document>()
                .withId(docDefinition.getId())
                .withProperty("mypk", sb.toString())
                .build();
        validateSuccess(createObservable, validator);
    }

    @Test(groups = { "simple" }, timeOut = TIMEOUT)
    public void readDocumentWithVeryLargePartitionKey() {
        Document docDefinition = getDocumentDefinition();
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < 100; i++) {
            sb.append(i).append("x");
        }
        docDefinition.set("mypk", sb.toString());

        Document createdDocument = TestSuiteBase.createDocument(client, createdDatabase.getId(), createdCollection.getId(), docDefinition);

        waitIfNeededForReplicasToCatchUp(clientBuilder);

        RequestOptions options = new RequestOptions();
        options.setPartitionKey(new PartitionKey(sb.toString()));
        Observable<ResourceResponse<Document>> readObservable = client.readDocument(createdDocument.getSelfLink(), options);

        ResourceResponseValidator<Document> validator = new ResourceResponseValidator.Builder<Document>()
                .withId(docDefinition.getId())
                .withProperty("mypk", sb.toString())
                .build();
        validateSuccess(readObservable, validator);
    }

    @Test(groups = { "simple" }, timeOut = TIMEOUT)
    public void createDocument_AlreadyExists() {
        Document docDefinition = getDocumentDefinition();

        client.createDocument(getCollectionLink(), docDefinition, null, false).toBlocking().single().getResource();

        Observable<ResourceResponse<Document>> createObservable = client
                .createDocument(getCollectionLink(), docDefinition, null, false);

        FailureValidator validator = new FailureValidator.Builder().resourceAlreadyExists().build();
        validateFailure(createObservable, validator);
    }

    @Test(groups = { "simple" }, timeOut = TIMEOUT)
    public void createDocumentTimeout() {
        Document docDefinition = getDocumentDefinition();

        Observable<ResourceResponse<Document>> createObservable = client
                .createDocument(getCollectionLink(), docDefinition, null, false)
                .timeout(1, TimeUnit.MICROSECONDS);

        FailureValidator validator = new FailureValidator.Builder().instanceOf(TimeoutException.class).build();

        validateFailure(createObservable, validator);
    }

    @Test(groups = { "simple" }, timeOut = TIMEOUT)
    public void readDocument() {
        Document docDefinition = getDocumentDefinition();

        Document document = client
                .createDocument(getCollectionLink(), docDefinition, null, false).toBlocking().single().getResource();

        waitIfNeededForReplicasToCatchUp(clientBuilder);

        RequestOptions options = new RequestOptions();
        options.setPartitionKey(new PartitionKey(document.get("mypk")));
        Observable<ResourceResponse<Document>> readObservable = client.readDocument(document.getSelfLink(), options);

        ResourceResponseValidator<Document> validator = new ResourceResponseValidator.Builder<Document>()
                .withId(document.getId())
                .build();
        validateSuccess(readObservable, validator);
    }

    @Test(groups = { "simple" }, timeOut = TIMEOUT)
    public void timestamp() throws Exception {
        Date before = new Date();
        Document docDefinition = getDocumentDefinition();
        Thread.sleep(1000);
        Document document = client
                .createDocument(getCollectionLink(), docDefinition, null, false).toBlocking().single().getResource();

        waitIfNeededForReplicasToCatchUp(clientBuilder);

        RequestOptions options = new RequestOptions();
        options.setPartitionKey(new PartitionKey(document.get("mypk")));
        Observable<ResourceResponse<Document>> readObservable = client.readDocument(document.getSelfLink(), options);
        Document readDocument = readObservable.toBlocking().single().getResource();
        Thread.sleep(1000);
        Date after = new Date();

        assertThat(readDocument.getTimestamp()).isAfterOrEqualsTo(before);
        assertThat(readDocument.getTimestamp()).isBeforeOrEqualsTo(after);
    }

    @Test(groups = { "simple" }, timeOut = TIMEOUT)
    public void readDocument_DoesntExist() {
        Document docDefinition = getDocumentDefinition();

        Document document = client
                .createDocument(getCollectionLink(), docDefinition, null, false).toBlocking().single().getResource();

        RequestOptions options = new RequestOptions();
        options.setPartitionKey(new PartitionKey(document.get("mypk")));
        client.deleteDocument(document.getSelfLink(), options).toBlocking().first();

        waitIfNeededForReplicasToCatchUp(clientBuilder);

        options.setPartitionKey(new PartitionKey("looloo"));
        Observable<ResourceResponse<Document>> readObservable = client.readDocument(document.getSelfLink(), options);

        FailureValidator validator = new FailureValidator.Builder().instanceOf(DocumentClientException.class)
                .statusCode(404).build();
        validateFailure(readObservable, validator);
    }

    @Test(groups = { "simple" }, timeOut = TIMEOUT)
    public void deleteDocument() {
        Document docDefinition = getDocumentDefinition();

        Document document = client
                .createDocument(getCollectionLink(), docDefinition, null, false).toBlocking().single().getResource();

        RequestOptions options = new RequestOptions();
        options.setPartitionKey(new PartitionKey(document.get("mypk")));
        Observable<ResourceResponse<Document>> deleteObservable = client.deleteDocument(document.getSelfLink(), options);


        ResourceResponseValidator<Document> validator = new ResourceResponseValidator.Builder<Document>()
                .nullResource().build();
        validateSuccess(deleteObservable, validator);

        // attempt to read document which was deleted
        waitIfNeededForReplicasToCatchUp(clientBuilder);

        Observable<ResourceResponse<Document>> readObservable = client.readDocument(getDocumentLink(docDefinition.getId()), options);
        FailureValidator notFoundValidator = new FailureValidator.Builder().resourceNotFound().build();
        validateFailure(readObservable, notFoundValidator);
    }

    @Test(groups = { "simple" }, timeOut = TIMEOUT)
    public void deleteDocument_undefinedPK() {
        Document docDefinition = new Document();
        docDefinition.setId(UUID.randomUUID().toString());

        Document document = client
                .createDocument(getCollectionLink(), docDefinition, null, false).toBlocking().single().getResource();

        RequestOptions options = new RequestOptions();
        options.setPartitionKey(new PartitionKey(Undefined.Value()));
        Observable<ResourceResponse<Document>> deleteObservable = client.deleteDocument(document.getSelfLink(), options);

        ResourceResponseValidator<Document> validator = new ResourceResponseValidator.Builder<Document>()
                .nullResource().build();
        validateSuccess(deleteObservable, validator);

        // attempt to read document which was deleted
        waitIfNeededForReplicasToCatchUp(clientBuilder);

        Observable<ResourceResponse<Document>> readObservable = client.readDocument(getDocumentLink(docDefinition.getId()), options);
        FailureValidator notFoundValidator = new FailureValidator.Builder().resourceNotFound().build();
        validateFailure(readObservable, notFoundValidator);
    }

    @Test(groups = { "simple" }, timeOut = TIMEOUT)
    public void deleteDocument_DoesntExist() {
        Document docDefinition = getDocumentDefinition();

        Document document = client
                .createDocument(getCollectionLink(), docDefinition, null, false).toBlocking().single().getResource();

        RequestOptions options = new RequestOptions();
        options.setPartitionKey(new PartitionKey(document.get("mypk")));
        client.deleteDocument(document.getSelfLink(), options).toBlocking().single();

        // delete again
        Observable<ResourceResponse<Document>> deleteObservable = client.deleteDocument(document.getSelfLink(), options);

        FailureValidator validator = new FailureValidator.Builder().resourceNotFound().build();
        validateFailure(deleteObservable, validator);
    }

    @Test(groups = { "simple" }, timeOut = TIMEOUT)
    public void replaceDocument() {
        // create a document
        Document docDefinition = getDocumentDefinition();

        Document document = client
                .createDocument(getCollectionLink(), docDefinition, null, false).toBlocking().single().getResource();

        String newPropValue = UUID.randomUUID().toString();
        document.set("newProp", newPropValue);

        // replace document
        Observable<ResourceResponse<Document>> readObservable = client.replaceDocument(document, null);

        // validate
        ResourceResponseValidator<Document> validator = new ResourceResponseValidator.Builder<Document>()
                .withProperty("newProp", newPropValue).build();
        validateSuccess(readObservable, validator);
    }

    @Test(groups = { "simple" }, timeOut = TIMEOUT)
    public void replaceDocument_UsingDocumentLink() {
        // create a document
        Document docDefinition = getDocumentDefinition();

        Document document = client
                .createDocument(getCollectionLink(), docDefinition, null, false).toBlocking().single().getResource();

        String newPropValue = UUID.randomUUID().toString();
        document.set("newProp", newPropValue);

        // replace document
        Observable<ResourceResponse<Document>> readObservable = client.replaceDocument(document.getSelfLink(), document, null);

        // validate
        ResourceResponseValidator<Document> validator = new ResourceResponseValidator.Builder<Document>()
                .withProperty("newProp", newPropValue).build();
        validateSuccess(readObservable, validator);
    }

    @Test(groups = { "simple" }, timeOut = TIMEOUT)
    public void upsertDocument_CreateDocument() {
        // create a document
        Document docDefinition = getDocumentDefinition();


        // replace document
        Observable<ResourceResponse<Document>> upsertObservable = client.upsertDocument(getCollectionLink(),
                docDefinition, null, false);

        // validate
        ResourceResponseValidator<Document> validator = new ResourceResponseValidator.Builder<Document>()
                .withId(docDefinition.getId()).build();
        try {
            validateSuccess(upsertObservable, validator);
        } catch (Throwable error) {
            if (this.clientBuilder.configs.getProtocol() == Protocol.Tcp) {
                String message = String.format("Direct TCP test failure ignored: desiredConsistencyLevel=%s", this.clientBuilder.desiredConsistencyLevel);
                logger.info(message, error);
                throw new SkipException(message, error);
            }
            throw error;
        }
    }

    @Test(groups = { "simple" }, timeOut = TIMEOUT)
    public void upsertDocument_ReplaceDocument() {
        // create a document
        Document docDefinition = getDocumentDefinition();

        Document document = client
                .createDocument(getCollectionLink(), docDefinition, null, false).toBlocking().single().getResource();

        String newPropValue = UUID.randomUUID().toString();
        document.set("newProp", newPropValue);

        // replace document
        Observable<ResourceResponse<Document>> readObservable = client.upsertDocument
                (getCollectionLink(), document, null, true);

        // validate
        ResourceResponseValidator<Document> validator = new ResourceResponseValidator.Builder<Document>()
                .withProperty("newProp", newPropValue).build();
        try {
            validateSuccess(readObservable, validator);
        } catch (Throwable error) {
            if (this.clientBuilder.configs.getProtocol() == Protocol.Tcp) {
                String message = String.format("Direct TCP test failure ignored: desiredConsistencyLevel=%s", this.clientBuilder.desiredConsistencyLevel);
                logger.info(message, error);
                throw new SkipException(message, error);
            }
            throw error;
        }
    }

    @BeforeClass(groups = { "simple" }, timeOut = SETUP_TIMEOUT)
    public void beforeClass() {
        createdDatabase = SHARED_DATABASE;
        createdCollection = SHARED_MULTI_PARTITION_COLLECTION;
        client = clientBuilder.build();
    }

    @AfterClass(groups = { "simple" }, timeOut = SHUTDOWN_TIMEOUT, alwaysRun = true)
    public void afterClass() {
        safeClose(client);
    }

    private String getCollectionLink() {
        return createdCollection.getSelfLink();
    }

    private String getDocumentLink(String docId) {
        return "dbs/" + createdDatabase.getId() + "/colls/" + createdCollection.getId() + "/docs/" + docId;
    }

    private Document getDocumentDefinition() {
        String uuid = UUID.randomUUID().toString();
        Document doc = new Document(String.format("{ "
                + "\"id\": \"%s\", "
                + "\"mypk\": \"%s\", "
                + "\"sgmts\": [[6519456, 1471916863], [2498434, 1455671440]]"
                + "}"
                , uuid, uuid));
        return doc;
    }
}
