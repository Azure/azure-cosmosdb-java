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

import java.util.UUID;

import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.ResourceResponse;
import com.microsoft.azure.documentdb.Trigger;
import com.microsoft.azure.documentdb.TriggerOperation;
import com.microsoft.azure.documentdb.TriggerType;

import rx.Observable;

public class TriggerUpsertReplaceTest extends TestSuiteBase {

    public final static String DATABASE_ID = getDatabaseId(TriggerUpsertReplaceTest.class);

    private static AsyncDocumentClient houseKeepingClient;
    private static Database createdDatabase;
    private static DocumentCollection createdCollection;

    private AsyncDocumentClient.Builder clientBuilder;
    private AsyncDocumentClient client;

    @Factory(dataProvider = "clientBuilders")
    public TriggerUpsertReplaceTest(AsyncDocumentClient.Builder clientBuilder) {
        this.clientBuilder = clientBuilder;
    }

    @Test(groups = { "simple" }, timeOut = TIMEOUT)
    public void upsertTrigger() throws Exception {

        // create a trigger
        Trigger trigger = new Trigger();
        trigger.setId(UUID.randomUUID().toString());
        trigger.setBody("function() {var x = 10;}");
        trigger.setTriggerOperation(TriggerOperation.Create);
        trigger.setTriggerType(TriggerType.Pre);
        Trigger readBackTrigger = client.upsertTrigger(getCollectionLink(), trigger, null).toBlocking().single().getResource();
        
        // read trigger to validate creation
        Observable<ResourceResponse<Trigger>> readObservable = client.readTrigger(readBackTrigger.getSelfLink(), null);

        // validate trigger creation
        ResourceResponseValidator<Trigger> validatorForRead = new ResourceResponseValidator.Builder<Trigger>()
                .withId(readBackTrigger.getId())
                .withTriggerBody("function() {var x = 10;}")
                .withTriggerInternals(TriggerType.Pre, TriggerOperation.Create)
                .notNullEtag()
                .build();
        validateSuccess(readObservable, validatorForRead);
        
        //update trigger
        readBackTrigger.setBody("function() {var x = 11;}");

        Observable<ResourceResponse<Trigger>> updateObservable = client.upsertTrigger(getCollectionLink(), readBackTrigger, null);

        // validate trigger update
        ResourceResponseValidator<Trigger> validatorForUpdate = new ResourceResponseValidator.Builder<Trigger>()
                .withId(readBackTrigger.getId())
                .withTriggerBody("function() {var x = 11;}")
                .withTriggerInternals(TriggerType.Pre, TriggerOperation.Create)
                .notNullEtag()
                .build();
        validateSuccess(updateObservable, validatorForUpdate);   
    }

    @Test(groups = { "simple" }, timeOut = TIMEOUT)
    public void replaceTrigger() throws Exception {

        // create a trigger
        Trigger trigger = new Trigger();
        trigger.setId(UUID.randomUUID().toString());
        trigger.setBody("function() {var x = 10;}");
        trigger.setTriggerOperation(TriggerOperation.Create);
        trigger.setTriggerType(TriggerType.Pre);
        Trigger readBackTrigger = client.createTrigger(getCollectionLink(), trigger, null).toBlocking().single().getResource();
        
        // read trigger to validate creation
        Observable<ResourceResponse<Trigger>> readObservable = client.readTrigger(readBackTrigger.getSelfLink(), null);

        // validate trigger creation
        ResourceResponseValidator<Trigger> validatorForRead = new ResourceResponseValidator.Builder<Trigger>()
                .withId(readBackTrigger.getId())
                .withTriggerBody("function() {var x = 10;}")
                .withTriggerInternals(TriggerType.Pre, TriggerOperation.Create)
                .notNullEtag()
                .build();
        validateSuccess(readObservable, validatorForRead);
        
        //update trigger
        readBackTrigger.setBody("function() {var x = 11;}");

        Observable<ResourceResponse<Trigger>> updateObservable = client.replaceTrigger(readBackTrigger, null);

        // validate trigger replace
        ResourceResponseValidator<Trigger> validatorForUpdate = new ResourceResponseValidator.Builder<Trigger>()
                .withId(readBackTrigger.getId())
                .withTriggerBody("function() {var x = 11;}")
                .withTriggerInternals(TriggerType.Pre, TriggerOperation.Create)
                .notNullEtag()
                .build();
        validateSuccess(updateObservable, validatorForUpdate);   
    }

    
    @BeforeClass(groups = { "simple" }, timeOut = SETUP_TIMEOUT)
    public void beforeClass() {
        this.client = this.clientBuilder.build();       
    }

    @AfterClass(groups = { "simple" }, timeOut = SHUTDOWN_TIMEOUT)
    public void afterClass() {
        this.client.close();
    }

    @BeforeSuite(groups = { "simple" }, timeOut = SETUP_TIMEOUT)
    public static void beforeSuite() {
        houseKeepingClient = createGatewayRxDocumentClient().build();
        Database d = new Database();
        d.setId(DATABASE_ID);
        createdDatabase = safeCreateDatabase(houseKeepingClient, d);
        createdCollection = safeCreateCollection(houseKeepingClient, createdDatabase.getId(), getCollectionDefinitionSinglePartition());
    }
    
    private static DocumentCollection getCollectionDefinitionSinglePartition() {
        
        DocumentCollection collectionDefinition = new DocumentCollection();
        collectionDefinition.setId(UUID.randomUUID().toString());

        return collectionDefinition;
    }
    
    @AfterSuite(groups = { "simple" }, timeOut = SETUP_TIMEOUT)
    public static void afterSuite() {

        deleteDatabase(houseKeepingClient, createdDatabase.getId());
        houseKeepingClient.close();
    }

    private String getCollectionLink() {
        return createdCollection.getSelfLink();
    }
}
