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

import com.microsoft.azure.cosmosdb.ConnectionPolicy;
import com.microsoft.azure.cosmosdb.ConsistencyLevel;
import com.microsoft.azure.cosmosdb.Database;
import com.microsoft.azure.cosmosdb.DocumentCollection;
import com.microsoft.azure.cosmosdb.FeedResponse;
import com.microsoft.azure.cosmosdb.ResourceResponse;
import com.microsoft.azure.cosmosdb.RetryOptions;
import com.microsoft.azure.cosmosdb.SqlParameter;
import com.microsoft.azure.cosmosdb.SqlParameterCollection;
import com.microsoft.azure.cosmosdb.SqlQuerySpec;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import com.microsoft.azure.cosmosdb.rx.TestConfigurations;
import org.testng.Assert;
import org.testng.annotations.Test;
import rx.Observable;

import java.util.List;

import static org.testng.Assert.assertNotNull;

public class MappingManagerInternalTest {


    private final static int LARGE_TIMEOUT = 30000;


    @Test(groups = { "internal" }, timeOut = LARGE_TIMEOUT )
    public void shouldCreateDatabaseCollection() {

        ConnectionPolicy policy = new ConnectionPolicy();
        RetryOptions retryOptions = new RetryOptions();
        retryOptions.setMaxRetryAttemptsOnThrottledRequests(Integer.MAX_VALUE);
        retryOptions.setMaxRetryWaitTimeInSeconds(LARGE_TIMEOUT);
        policy.setRetryOptions(retryOptions);

        AsyncDocumentClient client = new AsyncDocumentClient.Builder()
                .withServiceEndpoint(TestConfigurations.HOST)
                .withMasterKey(TestConfigurations.MASTER_KEY)
                .withConnectionPolicy(ConnectionPolicy.GetDefault())
                .withConsistencyLevel(ConsistencyLevel.Session)
                .build();

        MappingManager mappingManager = MappingManager.of(client);

        Mapper<Person> mapper = mappingManager.mapper(Person.class);

        assertNotNull(mapper);
        String databaseName = EntityMetadata.of(Person.class).getDatabaseLink();
        String collectionName = EntityMetadata.of(Person.class).getCollectionName();


        Observable<ResourceResponse<Database>> databaseReadObs = client.readDatabase(databaseName, null);
        ResourceResponse<Database> database = databaseReadObs.single().toBlocking().first();
        Assert.assertEquals(200, database.getStatusCode());

        FeedResponse<DocumentCollection> documentCollectionFeedResponse = client.queryCollections(databaseName,
                new SqlQuerySpec("SELECT * FROM r where r.id = @id",
                        new SqlParameterCollection(
                                new SqlParameter("@id", collectionName))), null)
                .toBlocking().first();

        List<DocumentCollection> results = documentCollectionFeedResponse.getResults();
        Assert.assertEquals(1, results.size());
        DocumentCollection documentCollection = results.get(0);
        Assert.assertEquals("Person", documentCollection.getId());
    }

}