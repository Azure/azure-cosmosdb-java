/**
 * The MIT License (MIT)
 * Copyright (c) 2016 Microsoft Corporation
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
import com.microsoft.azure.documentdb.UserDefinedFunction;

import rx.Observable;

public class UserDefinedFunctionUpsertReplaceTest extends TestSuiteBase {

    public final static String DATABASE_ID = getDatabaseId(UserDefinedFunctionUpsertReplaceTest.class);

    private static AsyncDocumentClient houseKeepingClient;
    private static Database createdDatabase;
    private static DocumentCollection createdCollection;

    private AsyncDocumentClient.Builder clientBuilder;
    private AsyncDocumentClient client;

    @Factory(dataProvider = "clientBuilders")
    public UserDefinedFunctionUpsertReplaceTest(AsyncDocumentClient.Builder clientBuilder) {
        this.clientBuilder = clientBuilder;
    }

    @Test(groups = { "simple" }, timeOut = TIMEOUT)
    public void upsertUserDefinedFunction() throws Exception {

        // create a udf
        UserDefinedFunction udf = new UserDefinedFunction();
        udf.setId(UUID.randomUUID().toString());
        udf.setBody("function() {var x = 10;}");
        UserDefinedFunction readBackUdf = client.upsertUserDefinedFunction(getCollectionLink(), udf, null).toBlocking().single().getResource();
        
        // read udf to validate creation
        Observable<ResourceResponse<UserDefinedFunction>> readObservable = client.readUserDefinedFunction(readBackUdf.getSelfLink(), null);

        // validate udf create
        ResourceResponseValidator<UserDefinedFunction> validatorForRead = new ResourceResponseValidator.Builder<UserDefinedFunction>()
                .withId(readBackUdf.getId())
                .withBody("function() {var x = 10;}", UserDefinedFunction.class)
                .notNullEtag()
                .build();
        validateSuccess(readObservable, validatorForRead);
        
        //update udf
        readBackUdf.setBody("function() {var x = 11;}");

        Observable<ResourceResponse<UserDefinedFunction>> updateObservable = client.upsertUserDefinedFunction(getCollectionLink(), readBackUdf, null);

        // validate udf update
        ResourceResponseValidator<UserDefinedFunction> validatorForUpdate = new ResourceResponseValidator.Builder<UserDefinedFunction>()
                .withId(readBackUdf.getId())
                .withBody("function() {var x = 11;}", UserDefinedFunction.class)
                .notNullEtag()
                .build();
        validateSuccess(updateObservable, validatorForUpdate);   
    }

    @Test(groups = { "simple" }, timeOut = TIMEOUT)
    public void replaceUserDefinedFunction() throws Exception {

        // create a udf
        UserDefinedFunction udf = new UserDefinedFunction();
        udf.setId(UUID.randomUUID().toString());
        udf.setBody("function() {var x = 10;}");        
        UserDefinedFunction readBackUdf = client.createUserDefinedFunction(getCollectionLink(), udf, null).toBlocking().single().getResource();
        
        // read udf to validate creation
        Observable<ResourceResponse<UserDefinedFunction>> readObservable = client.readUserDefinedFunction(readBackUdf.getSelfLink(), null);

        // validate udf creation
        ResourceResponseValidator<UserDefinedFunction> validatorForRead = new ResourceResponseValidator.Builder<UserDefinedFunction>()
                .withId(readBackUdf.getId())
                .withBody("function() {var x = 10;}", UserDefinedFunction.class)
                .notNullEtag()
                .build();
        validateSuccess(readObservable, validatorForRead);
        
        //update udf
        readBackUdf.setBody("function() {var x = 11;}");

        Observable<ResourceResponse<UserDefinedFunction>> replaceObservable = client.replaceUserDefinedFunction(readBackUdf, null);

        //validate udf replace
        ResourceResponseValidator<UserDefinedFunction> validatorForReplace = new ResourceResponseValidator.Builder<UserDefinedFunction>()
                .withId(readBackUdf.getId())
                .withBody("function() {var x = 11;}", UserDefinedFunction.class)
                .notNullEtag()
                .build();
        validateSuccess(replaceObservable, validatorForReplace);   
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
        createdCollection = safeCreateCollection(houseKeepingClient, createdDatabase.getSelfLink(), getCollectionDefinitionSinglePartition());
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
