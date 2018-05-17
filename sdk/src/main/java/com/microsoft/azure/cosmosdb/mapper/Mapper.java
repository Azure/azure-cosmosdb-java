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

import com.microsoft.azure.cosmosdb.FeedOptions;
import com.microsoft.azure.cosmosdb.RequestOptions;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import rx.Observable;

import java.util.List;

/**
 * An object handling the mapping of a particular class.
 *
 * @param <T> the entity class
 */
public interface Mapper<T> {

    /**
     * Creates an entity.
     *
     * @param entity the entity to be created in the database
     * @return an {@link Observable} containing the single resource response with the created document or an error.
     * @throws NullPointerException when entity is null
     */
    Observable<T> create(T entity);

    /**
     * Creates entities in database.
     *
     * @param entities the entities to be created in the database
     * @return an {@link Observable} containing the merged resource response with the created document or an error.
     * @throws NullPointerException when either entities is null or there is any null element
     */
    Observable<List<T>> create(Iterable<T> entities);

    /**
     * Updates an entity using {@link AsyncDocumentClient#replaceDocument(String, Object, RequestOptions)}
     *
     * @param entity the entity to be updated in the database
     * @return an {@link Observable} containing the single resource response with the created document or an error.
     * @throws NullPointerException when entity is null
     */
    Observable<T> update(T entity);

    /**
     * Updates an entity using {@link AsyncDocumentClient#replaceDocument(String, Object, RequestOptions)}
     * then merge the result
     *
     * @param entities the entities to be updated in the database
     * @return an {@link Observable} containing the merged resource response with the created document or an error.
     * @throws NullPointerException when either entities is null or there is any null element
     */
    Observable<List<T>> update(Iterable<T> entities);

    /**
     * Finds entity by id
     *
     * @param id the entity id
     * @return an {@link Observable} containing the single resource response with the created document or an error.
     * @throws NullPointerException when id is null
     */
    Observable<T> findById(String id);

    /**
     * Finds entities by ids
     *
     * @param ids the entities ids
     * @return an {@link Observable} containing the merged resource response with the created document or an error.
     * @throws NullPointerException when either ids is null or there is any null id
     */
    Observable<List<T>> findById(Iterable<String> ids);

    /**
     * Deletes entity by id
     *
     * @param id the entity id
     * @return an {@link Observable} containing the single resource response with the created document or an error.
     * @throws NullPointerException when id is null
     */
    Observable<Void> deleteById(String id);

    /**
     * Deletes entities by ids
     *
     * @param ids the entities ids
     * @return an {@link Observable} containing the single resource response with the created document or an error.
     * @throws NullPointerException when either ids is null or there is any null id
     */
    Observable<List<Void>> deleteById(Iterable<String> ids);

    /**
     * Searches entities by query
     *
     * @param query        the String query
     * @param queryOptions the query options
     * @return an {@link Observable} containing the single resource response with the created document or an error.
     * @throws NullPointerException when either query or queryOptions are null
     */
    Observable<List<T>> query(String query, FeedOptions queryOptions);

}
