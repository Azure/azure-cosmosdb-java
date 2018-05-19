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
import rx.Observable;

import java.util.Arrays;
import java.util.List;

import static org.testng.Assert.assertTrue;

public class RepositoryTest extends AbstractMapperClass {


    private PersonRepository repository = MappingManager.of(client).repository(PersonRepository.class);


    @Override
    Mapper<Person> getMapper() {
        return repository;
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void shouldReturnErrorWhenMethodIsNotSupported() {
        repository.methodHasNotSupport();
    }

    @Test(groups = {"internal"}, timeOut = LARGE_TIMEOUT)
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
        Observable<List<Person>> result = repository.findByName("Ada Lovelace");

        List<Person> people = result.toBlocking().first();

        assertTrue(people.size() > 1);
        assertTrue(people.stream().map(Person::getName).allMatch(ada.getName()::equals));
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void shouldReturnErrorWhenTheParameterValueIsNull() {
        repository.findByName(null);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void shouldReturnErrorWhenTheMethodIsInvalid() {
        repository.findByName(null);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void shouldReturnErrorWhen() {
        repository.invalidQuery("ops");
    }
}