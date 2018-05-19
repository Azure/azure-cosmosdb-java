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

import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MappingManagerTest {


    @Test(expectedExceptions = NullPointerException.class)
    public void shouldReturnNPEWhenClientIsNull() {
        MappingManager.of(null);
    }

    @Test
    public void shouldReturnMappingManager() {
        AsyncDocumentClient client = Mockito.mock(AsyncDocumentClient.class);
        MappingManager mappingManager = MappingManager.of(client);
        Assert.assertNotNull(mappingManager);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void shouldReturnErrorWhenEntityClassIsNull() {
        AsyncDocumentClient client = Mockito.mock(AsyncDocumentClient.class);
        MappingManager mappingManager = MappingManager.of(client);
        mappingManager.mapper(null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void shouldReturnErrorWhenEntityhasNotAnnotation() {
        AsyncDocumentClient client = Mockito.mock(AsyncDocumentClient.class);
        MappingManager mappingManager = MappingManager.of(client);
        mappingManager.mapper(User.class);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void shouldReturnErrorWhenEntityHasDatabaseNameBlank() {
        AsyncDocumentClient client = Mockito.mock(AsyncDocumentClient.class);
        MappingManager mappingManager = MappingManager.of(client);
        mappingManager.mapper(Family.class);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void shouldShouldReturnErrorWhenRepositoryIsNull() {
        AsyncDocumentClient client = Mockito.mock(AsyncDocumentClient.class);
        MappingManager mappingManager = MappingManager.of(client);
        mappingManager.repository(null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void shouldShouldReturnErrorWhenRepositoryIsAClass() {
        AsyncDocumentClient client = Mockito.mock(AsyncDocumentClient.class);
        MappingManager mappingManager = MappingManager.of(client);
        mappingManager.repository(UserRepository.class);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void shouldReturnErrorWhenRepositoryHasEntityNotAnnotated() {
        AsyncDocumentClient client = Mockito.mock(AsyncDocumentClient.class);
        MappingManager mappingManager = MappingManager.of(client);
        mappingManager.repository(UserRepository2.class);
    }


    public class User {

    }

    public abstract class UserRepository implements Repository<User> {

    }

    public interface UserRepository2 extends Repository<User> {

    }


    @Entity(databaseName = "")
    public class Family {

    }


}