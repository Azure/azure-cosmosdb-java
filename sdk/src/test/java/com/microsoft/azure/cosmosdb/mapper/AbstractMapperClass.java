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
import com.microsoft.azure.cosmosdb.DocumentClientException;
import com.microsoft.azure.cosmosdb.FeedOptions;
import com.microsoft.azure.cosmosdb.SqlParameter;
import com.microsoft.azure.cosmosdb.SqlParameterCollection;
import com.microsoft.azure.cosmosdb.SqlQuerySpec;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import com.microsoft.azure.cosmosdb.rx.TestConfigurations;
import org.testng.annotations.Test;
import rx.Observable;

import java.util.Arrays;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public abstract class AbstractMapperClass {

    private final static int LARGE_TIMEOUT = 30000;

    protected final AsyncDocumentClient client = new AsyncDocumentClient.Builder()
            .withServiceEndpoint(TestConfigurations.HOST)
            .withMasterKey(TestConfigurations.MASTER_KEY)
            .withConnectionPolicy(ConnectionPolicy.GetDefault())
            .withConsistencyLevel(ConsistencyLevel.Session)
            .build();


    abstract Mapper<Person> getMapper();

    @Test(groups = {"internal"}, timeOut = LARGE_TIMEOUT)
    public void shouldCreateEntity() {
        Person ada = new Person();
        ada.setId("ada" + System.currentTimeMillis());
        ada.setName("Ada Lovelace");
        ada.setAge(20);

        Observable<Person> observable = getMapper().save(ada);

        assertNotNull(observable);

        Person person = observable.toBlocking().first();

        assertNotNull(person);

        assertEquals(ada.getName(), person.getName());
        assertEquals(ada.getAge(), person.getAge());
        assertEquals(ada.getId(), person.getId());

    }

    @Test(groups = {"internal"}, expectedExceptions = NullPointerException.class)
    public void shouldReturnErrorWhenCreateHasEntityNull() {
        getMapper().save((Person) null);
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

        Observable<List<Person>> observable = getMapper().save(Arrays.asList(ada, marie));

        assertNotNull(observable);

        List<Person> people = observable.toBlocking().first();

        assertNotNull(people);

        assertTrue(people.stream().map(Person::getId)
                .allMatch(id -> id.equals(ada.getId()) || id.equals(marie.getId())));
    }

    @Test(groups = {"internal"}, expectedExceptions = NullPointerException.class)
    public void shouldReturnErrorWhenEntitiesIsNull() {
        getMapper().save((Iterable<Person>) null);
    }

    @Test(groups = {"internal"}, expectedExceptions = NullPointerException.class)
    public void shouldReturnErrorWhenEntitiesHasANullElement() {
        Person ada = new Person();
        ada.setId("ada" + System.currentTimeMillis());
        ada.setName("Ada Lovelace");
        ada.setAge(20);

        getMapper().save(Arrays.asList(ada, null));
    }


    @Test(groups = {"internal"})
    public void shouldUpdateEntity() {
        Person ada = new Person();
        ada.setId("ada" + System.currentTimeMillis());
        ada.setName("Ada Lovelace");
        ada.setAge(20);

        getMapper().save(ada).toBlocking().first();

        ada.setName("Ada Lovelace update");

        Person person = getMapper().save(ada).toBlocking().first();

        assertEquals(ada.getName(), person.getName());
        assertEquals(ada.getId(), person.getId());
        assertEquals(ada.getAge(), person.getAge());
    }

    @Test(groups = {"internal"}, expectedExceptions = NullPointerException.class)
    public void shouldReturnErrorWhenFindByIdHasNullId() {
        getMapper().findById((String) null);
    }

    @Test(groups = {"internal"}, expectedExceptions = NullPointerException.class)
    public void shouldReturnErrorWhenFindByIdHasNullIds() {
        getMapper().findById((Iterable<String>) null);
    }

    @Test(groups = {"internal"}, expectedExceptions = NullPointerException.class)
    public void shouldReturnErrorWhenFindByIdHasANullElement() {

        getMapper().findById(Arrays.asList("id", null));
    }

    @Test(groups = {"internal"}, timeOut = LARGE_TIMEOUT)
    public void shouldReturnErrorWhenIdDoesNotExist() {
        Observable<Person> observable = getMapper().findById("not_found");
        try {
            observable.single().toBlocking().first();
        } catch (Exception ex) {
            if (ex.getCause() instanceof DocumentClientException) {
                return;
            }
        }

        assert false;
    }

    @Test(groups = {"internal"}, timeOut = LARGE_TIMEOUT)
    public void shouldFindById() {

        Person ada = new Person();
        ada.setId("ada" + System.currentTimeMillis());
        ada.setName("Ada Lovelace");
        ada.setAge(20);

        getMapper().save(ada).toBlocking().first();

        Observable<Person> result = getMapper().findById(ada.getId());

        Person person = result.single().toBlocking().first();

        assertEquals(ada.getName(), person.getName());
        assertEquals(ada.getId(), person.getId());
        assertEquals(ada.getAge(), person.getAge());
    }

    @Test(groups = {"internal"}, timeOut = LARGE_TIMEOUT)
    public void shouldFindByIds() {

        Person ada = new Person();
        ada.setId("ada" + System.currentTimeMillis());
        ada.setName("Ada Lovelace");
        ada.setAge(20);

        Person marie = new Person();
        marie.setId("marie" + System.currentTimeMillis());
        marie.setName("Marie Curie");
        marie.setAge(20);

        Observable<List<Person>> observable = getMapper().save(Arrays.asList(ada, marie));

        assertNotNull(observable);

        observable.toBlocking().first();


        Observable<List<Person>> result = getMapper().findById(Arrays.asList(ada.getId(), marie.getId()));

        List<Person> people = result.toBlocking().first();

        assertNotNull(people);

        assertTrue(people.stream().map(Person::getId)
                .allMatch(id -> id.equals(ada.getId()) || id.equals(marie.getId())));
    }

    @Test(groups = {"internal"}, expectedExceptions = NullPointerException.class)
    public void shouldReturnErrorWhenDeleteByIdHasNullId() {
        getMapper().deleteById((String) null);
    }

    @Test(groups = {"internal"}, expectedExceptions = NullPointerException.class)
    public void shouldReturnErrorWhenDeleteByHasNullIds() {
        getMapper().deleteById((Iterable<String>) null);
    }

    @Test(groups = {"internal"}, expectedExceptions = NullPointerException.class)
    public void shouldReturnErrorWhenDeleteByHasANullElement() {
        getMapper().deleteById(Arrays.asList("id", null));
    }


    @Test(groups = {"internal"})
    public void shouldReturnErrorWhenDeleteDoesNotRemoveEntity() {
        Observable<Void> observable = getMapper().deleteById("not_found");
        try {
            observable.single().toBlocking().first();
        } catch (Exception ex) {
            if (ex.getCause() instanceof DocumentClientException) {
                return;
            }
        }

        assert false;
    }

    @Test(groups = {"internal"})
    public void shouldRemoveById() {
        Person ada = new Person();
        ada.setId("ada" + System.currentTimeMillis());
        ada.setName("Ada Lovelace");
        ada.setAge(20);

        getMapper().save(ada).toBlocking().first();

        Observable<Void> observable = getMapper().deleteById(ada.getId());
        observable.toBlocking().first();

        try {
            getMapper().findById(ada.getId()).toBlocking().first();
        } catch (RuntimeException ex) {
            if (ex.getCause() instanceof DocumentClientException) {
                return;
            }
        }

        assert false;

    }

    @Test(groups = {"internal"}, expectedExceptions = NullPointerException.class)
    public void shouldReturnErrorWhenQueryHasNullStringQuery() {

        FeedOptions queryOptions = new FeedOptions();
        getMapper().query((String) null, queryOptions);
    }

    @Test(groups = {"internal"}, expectedExceptions = NullPointerException.class)
    public void shouldReturnErrorWhenQueryHasNullQueryOptions() {
        getMapper().query("select * from c", null);
    }

    @Test(groups = {"internal"})
    public void shouldExecuteQuery() {

        Person ada = new Person();
        ada.setId("ada" + System.currentTimeMillis());
        ada.setName("Ada Lovelace");
        ada.setAge(20);

        Person marie = new Person();
        marie.setId("marie" + System.currentTimeMillis());
        marie.setName("Marie Curie");
        marie.setAge(20);

        getMapper().save(Arrays.asList(ada, marie)).toCompletable().await();

        FeedOptions queryOptions = new FeedOptions();
        queryOptions.setMaxItemCount(100);

        Observable<List<Person>> result =
                getMapper().query("SELECT * FROM Person WHERE Person.name = 'Ada Lovelace'", queryOptions);

        List<Person> people = result.toBlocking().first();

        assertTrue(people.size() > 1);
        assertTrue(people.stream().map(Person::getName).allMatch(ada.getName()::equals));
    }

    @Test(groups = {"internal"})
    public void shouldExecuteQueryBySpec() {

        Person ada = new Person();
        ada.setId("ada" + System.currentTimeMillis());
        ada.setName("Ada Lovelace");
        ada.setAge(20);

        Person marie = new Person();
        marie.setId("marie" + System.currentTimeMillis());
        marie.setName("Marie Curie");
        marie.setAge(20);

        getMapper().save(Arrays.asList(ada, marie)).toCompletable().await();

        FeedOptions queryOptions = new FeedOptions();
        queryOptions.setMaxItemCount(100);

        SqlQuerySpec query = new SqlQuerySpec("SELECT * FROM Person WHERE Person.name = @name",
                new SqlParameterCollection(new SqlParameter("@name", ada.getName())));

        Observable<List<Person>> result = getMapper().query(query, queryOptions);

        List<Person> people = result.toBlocking().first();

        assertTrue(people.size() > 1);
        assertTrue(people.stream().map(Person::getName).allMatch(ada.getName()::equals));
    }
}
