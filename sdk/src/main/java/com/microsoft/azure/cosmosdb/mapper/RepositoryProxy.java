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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

import static java.util.Arrays.asList;

/**
 * Proxy to {@link Repository} maker
 *
 * @param <T> the entity type
 */
class RepositoryProxy<T> implements InvocationHandler {

    private static final Set<Method> METHODS = new HashSet<>();

    static {
        METHODS.addAll(asList(Mapper.class.getDeclaredMethods()));
        METHODS.addAll(asList(Object.class.getDeclaredMethods()));
    }


    private final Mapper<T> mapper;

    RepositoryProxy(Mapper<T> mapper) {
        this.mapper = mapper;
    }

    @Override
    public Object invoke(Object instance, Method method, Object[] params) throws Throwable {

        if(METHODS.contains(method)){

        }

        return null;
    }
}
