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

import com.microsoft.azure.cosmosdb.Document;
import com.microsoft.azure.cosmosdb.FeedOptions;
import com.microsoft.azure.cosmosdb.FeedResponse;
import com.microsoft.azure.cosmosdb.RequestOptions;
import com.microsoft.azure.cosmosdb.ResourceResponse;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import rx.Observable;
import rx.functions.Func1;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;

/**
 * the default implementation of {@link Mapper}
 * @param <T> the entity type
 */
final class DefaultMapper<T> implements Mapper<T> {

    private final Class<T> entityClass;

    private final AsyncDocumentClient client;

    private final EntityMetadata entityMetadata;

    private final Func1<ResourceResponse<Document>, T> mapperFunction;


    public DefaultMapper(Class<T> entityClass, AsyncDocumentClient client, EntityMetadata entityMetadata) {
        this.entityClass = entityClass;
        this.client = client;
        this.entityMetadata = entityMetadata;
        mapperFunction = r -> r.getResource().toObject(entityClass);
    }

    @Override
    public Observable<T> save(T entity) {
        requireNonNull(entity, "entity is required");


        String collectionLink = entityMetadata.getCollectionLink();
        Observable<ResourceResponse<Document>> observable = client.upsertDocument(
                collectionLink, entity, new RequestOptions(), true);
        return observable.map(mapperFunction);
    }

    @Override
    public Observable<List<T>> save(Iterable<T> entities) {
        checkNullElements(entities);

        List<Observable<T>> observables = stream(entities.spliterator(), false).map(this::save).collect(toList());
        return Observable.merge(observables).toList();
    }


    @Override
    public Observable<T> findById(String id) {
        requireNonNull(id, "id is required");

        return client.readDocument(entityMetadata.getDocumentId(id), null)
                .map(mapperFunction);
    }

    @Override
    public Observable<List<T>> findById(Iterable<String> ids) {

        checkNullIds(ids);
        List<Observable<T>> observables = stream(ids.spliterator(), false)
                .map(this::findById)
                .collect(toList());

        return Observable.merge(observables).toList();
    }


    @Override
    public Observable<List<T>> query(String query, FeedOptions queryOptions) {
        requireNonNull(query, "query is required");
        requireNonNull(queryOptions, "queryOptions is required");

        Observable<FeedResponse<Document>> queryObservable =
                client.queryDocuments(entityMetadata.getCollectionLink(), query, queryOptions);

        return queryObservable.map(d -> d.getResults().stream()
                .map(r -> r.toObject(entityClass))
                .collect(toList()));
    }


    @Override
    public Observable<Void> deleteById(String id) {
        requireNonNull(id, "id is required");
        Observable<ResourceResponse<Document>> observable = client.deleteDocument(entityMetadata.getDocumentId(id), null);
        return observable.map(d -> null);


    }

    @Override
    public Observable<List<Void>> deleteById(Iterable<String> ids) {
        checkNullIds(ids);

        List<Observable<Void>> observables = stream(ids.spliterator(), false)
                .map(this::deleteById)
                .collect(toList());

        return Observable.merge(observables).toList();
    }

    private void checkNullIds(Iterable<String> ids) {
        requireNonNull(ids, "ids is required");

        if (stream(ids.spliterator(), false).anyMatch(Objects::isNull)) {
            throw new NullPointerException("The elements cannot be null");
        }
    }

    private void checkNullElements(Iterable<T> entities) {
        requireNonNull(entities, "entities are required");

        if (stream(entities.spliterator(), false).anyMatch(Objects::isNull)) {
            throw new NullPointerException("The elements cannot be null");
        }
    }
}
