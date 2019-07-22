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
import com.microsoft.azure.cosmosdb.internal.directconnectivity.Protocol;
import com.microsoft.azure.cosmosdb.rx.internal.Utils.ValueHolder;
import com.microsoft.azure.cosmosdb.rx.internal.query.OffsetContinuationToken;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;
import rx.Observable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class OffsetLimitQueryTests extends TestSuiteBase {
    private Database createdDatabase;
    private DocumentCollection createdCollection;
    private ArrayList<Document> docs = new ArrayList<>();

    private String partitionKey = "mypk";
    private int firstPk = 0;
    private int secondPk = 1;
    private String field = "field";

    private AsyncDocumentClient client;

    @Factory(dataProvider = "clientBuildersWithDirect")
    public OffsetLimitQueryTests(AsyncDocumentClient.Builder clientBuilder) {
        super(clientBuilder);
    }

    @Test(groups = {"simple"}, timeOut = TIMEOUT, dataProvider = "queryMetricsArgProvider")
    public void queryDocuments(boolean qmEnabled) {
        int skipCount = 4;
        int takeCount = 10;
        String query = "SELECT * from c OFFSET " + skipCount + " LIMIT " + takeCount;
        FeedOptions options = new FeedOptions();
        options.setMaxItemCount(5);
        options.setEnableCrossPartitionQuery(true);
        options.setPopulateQueryMetrics(qmEnabled);
        options.setMaxDegreeOfParallelism(2);
        Observable<FeedResponse<Document>> queryObservable = client.queryDocuments(createdCollection.getSelfLink(),
                query, options);

        FeedResponseListValidator<Document> validator =
                new FeedResponseListValidator.Builder<Document>().totalSize(takeCount).allPagesSatisfy(new FeedResponseValidator.Builder<Document>().requestChargeGreaterThanOrEqualTo(1.0).build())
                        .hasValidQueryMetrics(qmEnabled)
                        .build();

        try {
            validateQuerySuccess(queryObservable, validator, TIMEOUT);
        } catch (Throwable error) {
            if (this.clientBuilder().configs.getProtocol() == Protocol.Tcp) {
                String message = String.format("Direct TCP test failure: desiredConsistencyLevel=%s",
                        this.clientBuilder().desiredConsistencyLevel);
                logger.info(message, error);
                throw new SkipException(message, error);
            }
            throw error;
        }
    }

    @Test(groups = {"simple"}, timeOut = TIMEOUT)
    public void offsetContinuationTokenRoundTrips() {
        {
            // Positive
            OffsetContinuationToken offsetContinuationToken = new OffsetContinuationToken(42, "asdf");
            String serialized = offsetContinuationToken.toString();
            ValueHolder<OffsetContinuationToken> outOffsetContinuationToken = new ValueHolder<>();

            assertThat(OffsetContinuationToken.tryParse(serialized, outOffsetContinuationToken)).isTrue();
            OffsetContinuationToken deserialized = outOffsetContinuationToken.v;

            assertThat(deserialized.getOffset()).isEqualTo(42);
            assertThat(deserialized.getSourceToken()).isEqualTo("asdf");
        }
    }

    @Test(groups = {"simple"}, timeOut = TIMEOUT * 10)
    public void queryDocumentsWithTopContinuationTokens() {
        int skipCount = 3;
        int takeCount = 10;
        String query = "SELECT * from c OFFSET " + skipCount + " LIMIT " + takeCount;
        this.queryWithContinuationTokensAndPageSizes(query, new int[]{1, 5, 15}, takeCount);
    }

    private void queryWithContinuationTokensAndPageSizes(String query, int[] pageSizes, int takeCount) {
        for (int pageSize : pageSizes) {
            List<Document> receivedDocuments = this.queryWithContinuationTokens(query, pageSize);
            Set<String> actualIds = new HashSet<String>();
            for (Document document : receivedDocuments) {
                actualIds.add(document.getResourceId());
            }

            assertThat(actualIds.size()).describedAs("total number of results").isEqualTo(takeCount);
        }
    }

    private List<Document> queryWithContinuationTokens(String query, int pageSize) {
        String requestContinuation = null;
        List<String> continuationTokens = new ArrayList<String>();
        List<Document> receivedDocuments = new ArrayList<Document>();

        do {
            FeedOptions options = new FeedOptions();
            options.setMaxItemCount(pageSize);
            options.setEnableCrossPartitionQuery(true);
            options.setMaxDegreeOfParallelism(2);
            options.setRequestContinuation(requestContinuation);
            Observable<FeedResponse<Document>> queryObservable =
                    client.queryDocuments(createdCollection.getSelfLink(), query, options);

            Observable<FeedResponse<Document>> firstPageObservable = queryObservable.first();
            VerboseTestSubscriber<FeedResponse<Document>> testSubscriber = new VerboseTestSubscriber<>();
            firstPageObservable.subscribe(testSubscriber);
            testSubscriber.awaitTerminalEvent(TIMEOUT, TimeUnit.MILLISECONDS);
            testSubscriber.assertNoErrors();
            testSubscriber.assertCompleted();

            FeedResponse<Document> firstPage = testSubscriber.getOnNextEvents().get(0);
            requestContinuation = firstPage.getResponseContinuation();
            receivedDocuments.addAll(firstPage.getResults());

            continuationTokens.add(requestContinuation);
        } while (requestContinuation != null);

        return receivedDocuments;
    }

    public void bulkInsert(AsyncDocumentClient client) {
        generateTestData();

        for (Document doc : docs) {
            createDocument(client, createdDatabase.getId(), createdCollection.getId(), doc);
        }
    }

    public void generateTestData() {

        for (int i = 0; i < 10; i++) {
            Document d = new Document();
            d.setId(Integer.toString(i));
            d.set(field, i);
            d.set(partitionKey, firstPk);
            docs.add(d);
        }

        for (int i = 10; i < 20; i++) {
            Document d = new Document();
            d.setId(Integer.toString(i));
            d.set(field, i);
            d.set(partitionKey, secondPk);
            docs.add(d);
        }
    }

    @AfterClass(groups = {"simple"}, timeOut = SHUTDOWN_TIMEOUT, alwaysRun = true)
    public void afterClass() {
        safeClose(client);
    }

    @BeforeClass(groups = {"simple"}, timeOut = SETUP_TIMEOUT)
    public void beforeClass() throws Exception {
        client = this.clientBuilder().build();
        createdDatabase = SHARED_DATABASE;
        createdCollection = SHARED_MULTI_PARTITION_COLLECTION;
        truncateCollection(SHARED_MULTI_PARTITION_COLLECTION);

        bulkInsert(client);

        waitIfNeededForReplicasToCatchUp(clientBuilder());
    }
}
