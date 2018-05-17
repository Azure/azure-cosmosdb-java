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

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class EntityMetadataTest {


    @Test(expectedExceptions = IllegalArgumentException.class)
    public void shouldReturnErrorWhenEntityIsNotAnnotated() {
        EntityMetadata.of(User.class);
    }


    @Test(expectedExceptions = IllegalArgumentException.class)
    public void shouldReturnErrorWhenEntityHasDatabaseBlank() {
        EntityMetadata.of(Person.class);
    }

    @Test
    public void shouldReturnEntityWithSimpleClassAsCollectionName() {
        EntityMetadata metadata = EntityMetadata.of(Animal.class);
        assertEquals("database", metadata.getDatabaseName());
        assertEquals(Animal.class.getSimpleName(), metadata.getCollectionName());
    }

    @Test
    public void shouldReturnEntity() {
        EntityMetadata metadata = EntityMetadata.of(Work.class);
        assertEquals("database", metadata.getDatabaseName());
        assertEquals("usingAnnotation", metadata.getCollectionName());
    }

    @Test
    public void shouldReturnDocumentLink() {
        EntityMetadata metadata = EntityMetadata.of(Animal.class);
        assertEquals("/dbs/database/colls/Animal", metadata.getCollectionLink());
    }

    @Test
    public void shouldReturnDocumentId() {

        EntityMetadata metadata = EntityMetadata.of(Animal.class);
        assertEquals("/dbs/database/colls/Animal/docs/id", metadata.getDocumentId("id"));
    }

    static class User {

    }

    @Entity(databaseName = "")
    static class Person {

    }

    @Entity(databaseName = "database")
    static class Animal {

    }

    @Entity(databaseName = "database", collectionName = "usingAnnotation")
    static class Work {

    }
}