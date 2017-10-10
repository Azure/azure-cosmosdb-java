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
import com.microsoft.azure.documentdb.Permission;
import com.microsoft.azure.documentdb.PermissionMode;
import com.microsoft.azure.documentdb.ResourceResponse;
import com.microsoft.azure.documentdb.StoredProcedure;
import com.microsoft.azure.documentdb.User;

import rx.Observable;

public class PermissionCrudTest extends TestSuiteBase {

    public final static String DATABASE_ID = getDatabaseId(PermissionCrudTest.class);

    private static AsyncDocumentClient houseKeepingClient;
    private static Database createdDatabase;
    private static User createdUser;

    private AsyncDocumentClient.Builder clientBuilder;
    private AsyncDocumentClient client;

    @Factory(dataProvider = "clientBuilders")
    public PermissionCrudTest(AsyncDocumentClient.Builder clientBuilder) {
        this.clientBuilder = clientBuilder;
    }

    @Test(groups = { "simple" }, timeOut = TIMEOUT)
    public void createPermission() throws Exception {

        createdUser = safeCreateUser(houseKeepingClient, createdDatabase.getId(), getUserDefinition());
        //create permission
        Permission permission = new Permission();
        permission.setId(UUID.randomUUID().toString());
        permission.setPermissionMode(PermissionMode.Read);
        permission.setResourceLink("dbs/AQAAAA==/colls/AQAAAJ0fgTc=");
        

        Observable<ResourceResponse<Permission>> createObservable = client.createPermission(getUserLink(), permission, null);

        // validate permission creation
        ResourceResponseValidator<Permission> validator = new ResourceResponseValidator.Builder<Permission>()
                .withId(permission.getId())
                .withPermissionMode(PermissionMode.Read)
                .withPermissionResourceLink("dbs/AQAAAA==/colls/AQAAAJ0fgTc=")
                .notNullEtag()
                .build();
        validateSuccess(createObservable, validator);
    }

    @Test(groups = { "simple" }, timeOut = TIMEOUT)
    public void readPermission() throws Exception {
        createdUser = safeCreateUser(houseKeepingClient, createdDatabase.getId(), getUserDefinition());
        
        // create permission
        Permission permission = new Permission();
        permission.setId(UUID.randomUUID().toString());
        permission.setPermissionMode(PermissionMode.Read);
        permission.setResourceLink("dbs/AQAAAA==/colls/AQAAAJ0fgTc=");
        Permission readBackPermission = client.createPermission(getUserLink(), permission, null).toBlocking().single().getResource();

        // read Permission
        Observable<ResourceResponse<Permission>> readObservable = client.readPermission(readBackPermission.getSelfLink(), null);

        // validate permission read
        ResourceResponseValidator<Permission> validator = new ResourceResponseValidator.Builder<Permission>()
                .withId(permission.getId())
                .withPermissionMode(PermissionMode.Read)
                .withPermissionResourceLink("dbs/AQAAAA==/colls/AQAAAJ0fgTc=")
                .notNullEtag()
                .build();
        validateSuccess(readObservable, validator);
    }

    @Test(groups = { "simple" }, timeOut = TIMEOUT)
    public void deletePermission() throws Exception {
        
        createdUser = safeCreateUser(houseKeepingClient, createdDatabase.getId(), getUserDefinition());
        
        // create permission
        Permission permission = new Permission();
        permission.setId(UUID.randomUUID().toString());
        permission.setPermissionMode(PermissionMode.Read);
        permission.setResourceLink("dbs/AQAAAA==/colls/AQAAAJ0fgTc=");
        Permission readBackPermission = client.createPermission(getUserLink(), permission, null).toBlocking().single().getResource();
        
        // delete
        Observable<ResourceResponse<Permission>> deleteObservable = client.deletePermission(readBackPermission.getSelfLink(), null);

        // validate delete permission
        ResourceResponseValidator<Permission> validator = new ResourceResponseValidator.Builder<Permission>()
                .nullResource()
                .build();
        validateSuccess(deleteObservable, validator);

        //TODO validate after deletion the resource is actually deleted (not found)
    }

    @Test(groups = { "simple" }, timeOut = TIMEOUT)
    public void upsertPermission() throws Exception {
        
        createdUser = safeCreateUser(houseKeepingClient, createdDatabase.getId(), getUserDefinition());
        
        // create permission
        Permission permission = new Permission();
        permission.setId(UUID.randomUUID().toString());
        permission.setPermissionMode(PermissionMode.Read);
        permission.setResourceLink("dbs/AQAAAA==/colls/AQAAAJ0fgTc=");
        Permission readBackPermission = client.upsertPermission(getUserLink(), permission, null).toBlocking().single().getResource();

        // read Permission
        Observable<ResourceResponse<Permission>> readObservable = client.readPermission(readBackPermission.getSelfLink(), null);

        // validate permission creation
        ResourceResponseValidator<Permission> validatorForRead = new ResourceResponseValidator.Builder<Permission>()
                .withId(readBackPermission.getId())
                .withPermissionMode(PermissionMode.Read)
                .withPermissionResourceLink("dbs/AQAAAA==/colls/AQAAAJ0fgTc=")
                .notNullEtag()
                .build();
        validateSuccess(readObservable, validatorForRead);
        
        //update permission
        readBackPermission.setPermissionMode(PermissionMode.All);

        Observable<ResourceResponse<Permission>> updateObservable = client.upsertPermission(getUserLink(), readBackPermission, null);

        // validate permission update
        ResourceResponseValidator<Permission> validatorForUpdate = new ResourceResponseValidator.Builder<Permission>()
                .withId(readBackPermission.getId())
                .withPermissionMode(PermissionMode.All)
                .withPermissionResourceLink("dbs/AQAAAA==/colls/AQAAAJ0fgTc=")
                .notNullEtag()
                .build();
        validateSuccess(updateObservable, validatorForUpdate);   
    }
    
    @Test(groups = { "simple" }, timeOut = TIMEOUT)
    public void replacePermission() throws Exception {
        
        createdUser = safeCreateUser(houseKeepingClient, createdDatabase.getId(), getUserDefinition());
        
        // create permission
        Permission permission = new Permission();
        permission.setId(UUID.randomUUID().toString());
        permission.setPermissionMode(PermissionMode.Read);
        permission.setResourceLink("dbs/AQAAAA==/colls/AQAAAJ0fgTc=");
        Permission readBackPermission = client.createPermission(getUserLink(), permission, null).toBlocking().single().getResource();

        // read Permission
        Observable<ResourceResponse<Permission>> readObservable = client.readPermission(readBackPermission.getSelfLink(), null);

        // validate permission creation
        ResourceResponseValidator<Permission> validatorForRead = new ResourceResponseValidator.Builder<Permission>()
                .withId(readBackPermission.getId())
                .withPermissionMode(PermissionMode.Read)
                .withPermissionResourceLink("dbs/AQAAAA==/colls/AQAAAJ0fgTc=")
                .notNullEtag()
                .build();
        validateSuccess(readObservable, validatorForRead);
        
        //update permission
        readBackPermission.setPermissionMode(PermissionMode.All);

        Observable<ResourceResponse<Permission>> updateObservable = client.replacePermission(readBackPermission, null);

        // validate permission replace
        ResourceResponseValidator<Permission> validatorForUpdate = new ResourceResponseValidator.Builder<Permission>()
                .withId(readBackPermission.getId())
                .withPermissionMode(PermissionMode.All)
                .withPermissionResourceLink("dbs/AQAAAA==/colls/AQAAAJ0fgTc=")
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
    }

    @AfterSuite(groups = { "simple" }, timeOut = SETUP_TIMEOUT)
    public static void afterSuite() {

        deleteDatabase(houseKeepingClient, createdDatabase.getId());
        houseKeepingClient.close();
    }

    private static User getUserDefinition() {
        User user = new User();
        user.setId(UUID.randomUUID().toString());
        return user;
    }
    
    private String getUserLink() {
        return createdUser.getSelfLink();
    }
}
