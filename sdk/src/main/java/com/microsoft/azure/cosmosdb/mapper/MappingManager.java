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

import java.util.Objects;

/**
 * Mapping manager that obtains entity mappers.
 */
public interface MappingManager {

    /**
     * returns a Mapper instance. Also, check if both document and database do exist based on {@link Entity}
     * annotation otherwise will create.
     *
     * @param entityClass the entity class
     * @param <T>         the entity type
     * @return the Mapper instance
     * @throws NullPointerException     when entityClass is null
     * @throws IllegalArgumentException when the class is not annotated with {@link Entity}
     */
    <T> Mapper<T> mapper(Class<T> entityClass);


    /**
     * returns a {@link Repository} instance already implemented.
     * Also, check if both document and database do exist based on {@link Entity} annotation otherwise will create.
     *
     * @param repositoryClass the repository class
     * @param <E>             the entity class
     * @param <T>             the repository interface
     * @return a {@link Repository} instance
     * @throws NullPointerException     when repositoryClass is null
     * @throws IllegalArgumentException when the <b>E</b> class is not annotated with {@link Entity},
     *                                  when the <b>T</b> is not an interface,
     *                                  check {@link MappingManager#mapper(Class)}
     */
    <E, T extends Repository<E>> T repository(Class<T> repositoryClass);

    /**
     * returns a {@link MappingManager} instance
     *
     * @param client the AsyncDocumentClient client
     * @return a instance
     * @throws NullPointerException when client is null
     */
    static MappingManager of(AsyncDocumentClient client) {
        Objects.requireNonNull(client, "client is required");
        return new DefaultMappingManager(client);
    }


}
