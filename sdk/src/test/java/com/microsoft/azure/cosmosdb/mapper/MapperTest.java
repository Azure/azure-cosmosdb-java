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
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import com.microsoft.azure.cosmosdb.rx.TestConfigurations;
import org.testng.annotations.Test;
import rx.Observable;

import java.util.Arrays;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class MapperTest {

    private final static int LARGE_TIMEOUT = 30000;

    private AsyncDocumentClient client = new AsyncDocumentClient.Builder()
            .withServiceEndpoint(TestConfigurations.HOST)
            .withMasterKey(TestConfigurations.MASTER_KEY)
            .withConnectionPolicy(ConnectionPolicy.GetDefault())
            .withConsistencyLevel(ConsistencyLevel.Session)
            .build();

    private Mapper<Person> mapper = MappingManager.of(client).mapper(Person.class);


    @Test(groups = {"internal"}, timeOut = LARGE_TIMEOUT)
    public void shouldCreateEntity() {
        Person ada = new Person();
        ada.setId("ada" + System.currentTimeMillis());
        ada.setName("Ada Lovelace");
        ada.setAge(20);

        Observable<Person> observable = mapper.save(ada);

        assertNotNull(observable);

        Person person = observable.toBlocking().first();

        assertNotNull(person);

        assertEquals(ada.getName(), person.getName());
        assertEquals(ada.getAge(), person.getAge());
        assertEquals(ada.getId(), person.getId());

    }

    @Test(expectedExceptions = NullPointerException.class)
    public void shouldReturnErrorWhenCreateHasEntityNull() {
        mapper.save((Person) null);
    }

    @Test(groups = {"internal"}, timeOut = LARGE_TIMEOUT)
    public void shouldCreateEntities() {
        Person ada = new Person();
        ada.setId("ada" + System.currentTimeMillis());
        ada.setName("Ada Lovelace");
        ada.setAge(20);

        Person marie = new Person();
        marie.setId("marie" + System.currentTimeMillis());
        marie.setName("Marie Curie");
        marie.setAge(20);

        Observable<List<Person>> observable = mapper.save(Arrays.asList(ada, marie));

        assertNotNull(observable);

        List<Person> people = observable.toBlocking().first();

        assertNotNull(people);

        assertTrue(people.stream().map(Person::getId)
                .allMatch(id -> id.equals(ada.getId()) || id.equals(marie.getId())));
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void shouldReturnErrorWhenEntitiesIsNull() {
        mapper.save((Iterable<Person>) null);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void shouldReturnErrorWhenEntitiesHasANullElement() {
        Person ada = new Person();
        ada.setId("ada" + System.currentTimeMillis());
        ada.setName("Ada Lovelace");
        ada.setAge(20);

        mapper.save(Arrays.asList(ada, null));
    }


    @Test
    public void shouldUpdateEntity() {
        Person ada = new Person();
        ada.setId("ada" + System.currentTimeMillis());
        ada.setName("Ada Lovelace");
        ada.setAge(20);

        mapper.save(ada).toBlocking().first();

        ada.setName("Ada Lovelace update");

        Person person = mapper.save(ada).toBlocking().first();

        assertEquals(ada.getName(), person.getName());
        assertEquals(ada.getId(), person.getId());
        assertEquals(ada.getAge(), person.getAge());
    }
}