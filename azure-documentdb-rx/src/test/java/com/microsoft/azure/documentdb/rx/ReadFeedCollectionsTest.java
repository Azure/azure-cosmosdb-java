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

import java.net.URISyntaxException;
import java.util.UUID;

import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.FeedOptions;
import com.microsoft.azure.documentdb.rx.internal.ReadFeedInternal.*;

public class ReadFeedCollectionsTest extends TestSuiteBase {

    public final static String DATABASE_ID = getDatabaseId(ReadFeedCollectionsTest.class);

    private static AsyncDocumentClient houseKeepingClient;
    private static Database createdDatabase;

    private AsyncDocumentClient.Builder clientBuilder;
    private static AsyncDocumentClient client;

    private static FeedOptions feedOptions = new FeedOptions();

    @Factory(dataProvider = "gatewayOnlyBuilder")
    public ReadFeedCollectionsTest(AsyncDocumentClient.Builder clientBuilder) {
        this.clientBuilder = clientBuilder;
        feedOptions.setPageSize(1);
    }
   
    @Test(groups = { "simple" }, timeOut = TIMEOUT)
    public void readCollections() throws Exception {

        String databaseLink = "dbs/" + getDatabaseId();
        CountSubscriber<DocumentCollection> readCollectionsSubscriber = new CountSubscriber<DocumentCollection>(true);
        client.readCollections(databaseLink, feedOptions).toBlocking().subscribe(readCollectionsSubscriber.subscriber);

        int initialCount = readCollectionsSubscriber.count;

        DocumentCollection collection = new DocumentCollection();
        collection.setId(UUID.randomUUID().toString());

        CrudSubscriber<DocumentCollection> collectionCrudSubscriber = new CrudSubscriber<DocumentCollection>(client, feedOptions, "DocumentCollection", databaseLink);
        
        collectionCrudSubscriber.target = initialCount + 1;
        client.createCollection(databaseLink, collection, null).toBlocking().subscribe(collectionCrudSubscriber.subscriber);
        collectionCrudSubscriber.target = initialCount;
        client.deleteCollection(databaseLink + "/colls/" + collection.getId(), null).toBlocking().subscribe(collectionCrudSubscriber.subscriber);
    }
    
    
    @BeforeClass(groups = { "simple" }, timeOut = SETUP_TIMEOUT)
    public void beforeClass() throws URISyntaxException {
        client = clientBuilder.build();
    }

    @AfterClass(groups = { "simple" }, timeOut = SHUTDOWN_TIMEOUT)
    public void afterClass() {
        client.close();
    }

    @BeforeSuite(groups = { "simple" }, timeOut = SETUP_TIMEOUT)
    public static void beforeSuite() {
        houseKeepingClient = createGatewayRxDocumentClient().build();
        Database d = new Database();
        d.setId(DATABASE_ID);
        createdDatabase = safeCreateDatabase(houseKeepingClient, d);
    }

    @AfterSuite(groups = { "simple" }, timeOut = SETUP_TIMEOUT)
    public static void afterSuite() {

        deleteDatabase(houseKeepingClient, createdDatabase.getId());
        houseKeepingClient.close();
    }
    
    private String getDatabaseId() {
        return createdDatabase.getId();
    }
}
