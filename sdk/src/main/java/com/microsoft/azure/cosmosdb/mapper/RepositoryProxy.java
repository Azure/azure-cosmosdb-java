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
import com.microsoft.azure.cosmosdb.SqlParameter;
import com.microsoft.azure.cosmosdb.SqlParameterCollection;
import com.microsoft.azure.cosmosdb.SqlQuerySpec;
import org.apache.commons.lang3.StringUtils;
import rx.Observable;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

/**
 * Proxy to {@link Repository} maker
 *
 * @param <T> the entity type
 */
class RepositoryProxy<T> implements InvocationHandler {

    private static final Set<Method> MAPPER_METHODS = new HashSet<>();
    private static final Set<Method> OBJECT_METHODS = new HashSet<>();
    private static final FeedOptions QUERY_OPTIONS = new FeedOptions();
    private static final char PARAM_PREFIX = '@';

    static {
        MAPPER_METHODS.addAll(asList(Mapper.class.getDeclaredMethods()));
        OBJECT_METHODS.addAll(asList(Object.class.getDeclaredMethods()));
    }


    private final Mapper<T> mapper;

    RepositoryProxy(Mapper<T> mapper) {
        this.mapper = mapper;
    }

    @Override
    public Object invoke(Object instance, Method method, Object[] params) throws Throwable {

        if (MAPPER_METHODS.contains(method)) {
            try {
                return method.invoke(mapper, params);
            } catch (Exception ex) {
                throw ex.getCause();
            }

        } else if (OBJECT_METHODS.contains(method)) {
            return method.invoke(this, params);
        }

        Query query = method.getAnnotation(Query.class);
        if (Objects.nonNull(query)) {

            if (StringUtils.isBlank(query.value())) {
                throw new IllegalArgumentException("The value @Query cannot be blank");
            }

            if (returnIsValidQuery(method)) {
                return executeQuery(method, params, query);
            }
        }

        throw new UnsupportedOperationException(String.format("The method %s is not supported yet", method));
    }

    private Object executeQuery(Method method, Object[] params, Query query) {
        SqlParameterCollection sqlParameters = new SqlParameterCollection();
        Parameter[] parameters = method.getParameters();
        for (int index = 0; index < params.length; index++) {
            Parameter parameter = parameters[index];
            Param param = Optional.ofNullable(parameter.getAnnotation(Param.class))
                    .orElseThrow(() -> new IllegalArgumentException("When the method has @Query all parameters " +
                            "should have @Param"));

            sqlParameters.add(createParameter(param, params[index]));
        }

        return mapper.query(new SqlQuerySpec(query.value(), sqlParameters), QUERY_OPTIONS);
    }


    private SqlParameter createParameter(Param param, Object value) {
        return new SqlParameter(getParamName(param), requireNonNull(value, param.value() + " is required."));
    }

    private String getParamName(Param param) {
        String name = param.value();
        if (name.charAt(0) != PARAM_PREFIX) {
            return PARAM_PREFIX + name;
        }
        return name;
    }

    private boolean returnIsValidQuery(Method method) {
        Class<?> returnType = method.getReturnType();
        return Observable.class.equals(returnType);
    }
}
