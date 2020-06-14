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
import com.microsoft.azure.cosmosdb.DocumentCollection;
import com.microsoft.azure.cosmosdb.FeedOptions;
import com.microsoft.azure.cosmosdb.FeedResponse;
import com.microsoft.azure.cosmosdb.RequestOptions;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;
import rx.Observable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class QueryValidationTests extends TestSuiteBase {
    private static final int DEFAULT_NUM_DOCUMENTS = 1000;
    private static final int DEFAULT_PAGE_SIZE = 100;
    private Random random;
    private Database createdDatabase;
    private DocumentCollection createdCollection;
    private List<Document> createdDocuments = new ArrayList<>();

    private AsyncDocumentClient client;

    @Factory(dataProvider = "clientBuildersWithDirectSession")
    public QueryValidationTests(AsyncDocumentClient.Builder clientBuilder) {
        super(clientBuilder);
        random = new Random();
    }

    @Test(groups = {"simple"}, timeOut = TIMEOUT)
    public void orderByQuery() {
        /*
        The idea here is to query documents in pages, query all the documents(with pagesize as num_documents and compare
         the results.
         */
        String query = "select * from c order by c.propInt ASC";
        queryWithOrderByAndAssert(
                DEFAULT_PAGE_SIZE,
                DEFAULT_NUM_DOCUMENTS,
                query,
                "propInt",
                getDefaultCreatedCollectionLink(),
                createdDocuments);
    }

    @Test(groups = {"simple"}, timeOut = TIMEOUT)
    public void orderByQueryForLargeCollection() {
        RequestOptions requestOptions = new RequestOptions();
        requestOptions.setOfferThroughput(100000); //want to test scenario with large number physical partitions

        DocumentCollection collection = createCollection(
                client,
                SHARED_DATABASE.getId(),
                getCollectionDefinition(),
                requestOptions);

        String collectionLink = Utils.getCollectionNameLink(createdDatabase.getId(), collection.getId());

        List<String> partitionKeys = new ArrayList<>();
        partitionKeys.add((UUID.randomUUID().toString()));
        partitionKeys.add(UUID.randomUUID().toString());

        List<Document> documentsInserted = this.insertDocuments(
                DEFAULT_NUM_DOCUMENTS,
                partitionKeys,
                collectionLink);

        String query = String.format(
                "select * from c where c.mypk in ('%s', '%s') order by c.name DESC",
                partitionKeys.get(0),
                partitionKeys.get(1));

        queryWithOrderByAndAssert(
                DEFAULT_PAGE_SIZE,
                DEFAULT_NUM_DOCUMENTS,
                query,
                "name",
                collectionLink,
                documentsInserted);
    }

    private List<Document> queryWithContinuationTokens(String query, int pageSize, String collectionNameLink) {
        logger.info("querying: " + query);
        String requestContinuation = null;

        List<String> continuationTokens = new ArrayList<String>();
        List<Document> receivedDocuments = new ArrayList<Document>();
        do {
            FeedOptions options = new FeedOptions();
            options.setMaxItemCount(pageSize);
            options.setEnableCrossPartitionQuery(true);
            options.setMaxDegreeOfParallelism(2);
            options.setRequestContinuation(requestContinuation);
            Observable<FeedResponse<Document>> queryObservable = client.queryDocuments(collectionNameLink, query,
                                                                                       options);

            FeedResponse<Document> firstPage = queryObservable.first().toBlocking().single();
            requestContinuation = firstPage.getResponseContinuation();
            receivedDocuments.addAll(firstPage.getResults());
            continuationTokens.add(requestContinuation);
        } while (requestContinuation != null);

        return receivedDocuments;
    }

    @BeforeMethod(groups = {"simple"})
    public void beforeMethod() throws Exception {
        // add a cool off time
        TimeUnit.SECONDS.sleep(10);
    }

    @BeforeClass(groups = {"simple"}, timeOut = SETUP_TIMEOUT)
    public void beforeClass() throws Exception {
        client = this.clientBuilder().build();
        createdDatabase = SHARED_DATABASE;
        createdCollection = SHARED_MULTI_PARTITION_COLLECTION;
        truncateCollection(SHARED_MULTI_PARTITION_COLLECTION);

        createdDocuments = this.insertDocuments(
                DEFAULT_NUM_DOCUMENTS,
                null,
                getDefaultCreatedCollectionLink());
        int numberOfPartitions = client
                                         .readPartitionKeyRanges(getDefaultCreatedCollectionLink(), null)
                                         .flatMap(p -> Observable.from(p.getResults())).toList().toBlocking().single()
                                         .size();
    }

    private List<Document> insertDocuments(int documentCount, List<String> partitionKeys, String collectionNameLink) {
        List<Document> documentsToInsert = new ArrayList<>();

        for (int i = 0; i < documentCount; i++) {
            String partitionKey = partitionKeys == null ?
                    UUID.randomUUID().toString() :
                    partitionKeys.get(random.nextInt(partitionKeys.size()));

            documentsToInsert.add(getDocumentDefinition(UUID.randomUUID().toString(), partitionKey));
        }

        List<Document> documentsInserted = bulkInsertBlocking(client, collectionNameLink, documentsToInsert);

        waitIfNeededForReplicasToCatchUp(this.clientBuilder());

        return documentsInserted;
    }

    private Document getDocumentDefinition(String documentId, String partitionKey) {
        Document doc = new Document(String.format("{ "
                                                          + "\"id\": \"%s\", "
                                                          + "\"mypk\": \"%s\", "
                                                          + "\"propInt\": %s, "
                                                          + "\"name\": \"test-document\", "
                                                          + "\"sgmts\": [[6519456, 1471916863], [2498434, 1455671440]]"
                                                          + "}"
                , documentId, partitionKey, random.nextInt(DEFAULT_NUM_DOCUMENTS/2)));
        // Doing NUM_DOCUMENTS/2 just to ensure there will be good number of repetetions.
        return doc;
    }

    public String getDefaultCreatedCollectionLink() {
        return Utils.getCollectionNameLink(createdDatabase.getId(), createdCollection.getId());
    }

    private <T> List<String> sortDocumentsAndCollectResourceIds(
            List<Document> createdDocuments, String propName,
            Function<Document, T> extractProp, Comparator<T> comparer) {
        return createdDocuments.stream()
                       .filter(d -> d.getHashMap().containsKey(propName)) // removes undefined
                       .sorted((d1, d2) -> comparer.compare(extractProp.apply(d1), extractProp.apply(d2)))
                       .map(d -> d.getId()).collect(Collectors.toList());
    }

    private void queryWithOrderByAndAssert(
            int pageSize,
            int documentCount,
            String query,
            String orderByPropName,
            String collectionLink,
            List<Document> documentsInserted) {
        List<Document> documentsPaged = queryWithContinuationTokens(query, pageSize, collectionLink);
        List<Document> allDocuments = queryWithContinuationTokens(query, documentCount, collectionLink);

        Comparator<Integer> validatorComparator = Comparator.nullsFirst(Comparator.<Integer>naturalOrder());
        List<String> expectedResourceIds = sortDocumentsAndCollectResourceIds(documentsInserted,
                orderByPropName,
                d -> d.getInt(orderByPropName),
                validatorComparator);

        List<String> docIds1 = documentsPaged.stream().map(Document::getId).collect(Collectors.toList());
        List<String> docIds2 = allDocuments.stream().map(Document::getId).collect(Collectors.toList());

        assertThat(docIds2).containsExactlyInAnyOrderElementsOf(expectedResourceIds);
        assertThat(docIds1).containsExactlyElementsOf(docIds2);
    }

}
