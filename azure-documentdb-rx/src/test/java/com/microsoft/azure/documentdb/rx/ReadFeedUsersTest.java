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
import com.microsoft.azure.documentdb.FeedOptions;
import com.microsoft.azure.documentdb.User;
import com.microsoft.azure.documentdb.rx.internal.ReadFeedInternal.*;

public class ReadFeedUsersTest extends TestSuiteBase {

    public final static String DATABASE_ID = getDatabaseId(ReadFeedUsersTest.class);

    private static AsyncDocumentClient houseKeepingClient;
    private static Database createdDatabase;

    private AsyncDocumentClient.Builder clientBuilder;
    private static AsyncDocumentClient client;

    private static FeedOptions feedOptions = new FeedOptions();

    @Factory(dataProvider = "clientBuilders")
    public ReadFeedUsersTest(AsyncDocumentClient.Builder clientBuilder) {
        this.clientBuilder = clientBuilder;
        feedOptions.setPageSize(1);
    }
    
    @Test(groups = { "simple" }, timeOut = TIMEOUT)
    public void readUsers() throws Exception {

        String databaseLink = "dbs/" + getDatabaseId();
        CountSubscriber<User> readUsersSubscriber = new CountSubscriber<User>(true);
        client.readUsers(databaseLink, feedOptions).toBlocking().subscribe(readUsersSubscriber.subscriber);

        int initialCount = readUsersSubscriber.count;
       
        User user = new User();
        user.setId(UUID.randomUUID().toString());

        CrudSubscriber<User> userCrudSubscriber = new CrudSubscriber<User>(client, feedOptions, "User", databaseLink);
        
        userCrudSubscriber.target = initialCount + 1;
        client.createUser(databaseLink, user, null).toBlocking().subscribe(userCrudSubscriber.subscriber);
        userCrudSubscriber.target = initialCount;
        client.deleteUser(databaseLink + "/users/" + user.getId(), null).toBlocking().subscribe(userCrudSubscriber.subscriber);
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
