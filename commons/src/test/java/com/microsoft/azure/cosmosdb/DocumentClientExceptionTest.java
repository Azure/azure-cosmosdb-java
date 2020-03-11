package com.microsoft.azure.cosmosdb;/*
 * The MIT License (MIT)
 * Copyright (c) 2018 Microsoft Corporation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

import com.google.common.collect.ImmutableMap;
import com.microsoft.azure.cosmosdb.internal.InternalServerErrorException;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.GoneException;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.RequestTimeoutException;
import com.microsoft.azure.cosmosdb.rx.internal.BadRequestException;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.reactivex.netty.protocol.http.client.HttpResponseHeaders;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.google.common.base.Strings.lenientFormat;
import static com.microsoft.azure.cosmosdb.internal.HttpConstants.StatusCodes.BADREQUEST;
import static com.microsoft.azure.cosmosdb.internal.HttpConstants.StatusCodes.GONE;
import static com.microsoft.azure.cosmosdb.internal.HttpConstants.StatusCodes.INTERNAL_SERVER_ERROR;
import static com.microsoft.azure.cosmosdb.internal.HttpConstants.StatusCodes.REQUEST_TIMEOUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.FileAssert.fail;

public class DocumentClientExceptionTest {

    @Test(groups = { "unit" })
    public void canEnumerateWhileUpdatingHeaders(Method method) {

        final DocumentClientException dce = new DocumentClientException(0, method.getName());
        final ExecutorService threadPool = Executors.newFixedThreadPool(4);

        final MeterRegistry meterRegistry = new SimpleMeterRegistry();

        final DistributionSummary[] summary = {
            DistributionSummary.builder("responseHeaders.size").register(meterRegistry),
            DistributionSummary.builder("requestHeaders.size").register(meterRegistry),
        };

        final List<Callable<Boolean>> callables = Arrays.asList(
            () -> {
                for (int i = 0; i < 1000; i++) {
                    final Map<String, String> headers = dce.getResponseHeaders();
                    final String string = headers.toString();
                    final double size = headers.size();
                    summary[0].record(size);
                }
                return Boolean.TRUE;
            },
            () -> {

                for (int i = 0; i < 5000; i++) {
                    dce.getResponseHeaders().put("foo." + i, "bar");
                }

                Thread.sleep(1);

                for (int i = 0; i < 5000; i++) {
                    dce.getResponseHeaders().remove("foo." + i);
                }

                return Boolean.TRUE;
            },
            () -> {
                for (int i = 0; i < 1000; i++) {
                    final Map<String, String> headers = dce.getRequestHeaders();
                    final String string = headers.toString();
                    final double size = headers.size();
                    summary[1].record(size);
                }
                return Boolean.TRUE;
            },
            () -> {
                for (int i = 0; i < 1000; i++) {
                    dce.setRequestHeaders(dce.getResponseHeaders());
                }
                return Boolean.TRUE;
            });

        final List<Future<Boolean>> futures;

        try {
            futures = threadPool.invokeAll(callables);
        } catch (InterruptedException error) {
            fail(lenientFormat("unexpected %s", error), error);
            return;
        }

        for (Future<Boolean> future : futures) {
            try {
                assertTrue(future.get());
            } catch (ExecutionException | InterruptedException error) {
                fail(lenientFormat("unexpected %s", error), error);
                return;
            }
        }

        System.out.println(summary[0].takeSnapshot());
        System.out.println(summary[1].takeSnapshot());
    }

    @Test(groups = { "unit" })
    public void headerNotNull1() {
        DocumentClientException dce = new DocumentClientException(0);
        assertThat(dce.getResponseHeaders()).isNotNull();
        assertThat(dce.getResponseHeaders()).isEmpty();
    }

    @Test(groups = { "unit" })
    public void headerNotNull2() {
        DocumentClientException dce = new DocumentClientException(0, "dummy");
        assertThat(dce.getResponseHeaders()).isNotNull();
        assertThat(dce.getResponseHeaders()).isEmpty();
    }

    @Test(groups = { "unit" })
    public void headerNotNull3() {
        DocumentClientException dce = new DocumentClientException(0, new RuntimeException());
        assertThat(dce.getResponseHeaders()).isNotNull();
        assertThat(dce.getResponseHeaders()).isEmpty();
    }

    @Test(groups = { "unit" })
    public void headerNotNull4() {
        DocumentClientException dce = new DocumentClientException(0, (Error) null, (Map) null);
        assertThat(dce.getResponseHeaders()).isNotNull();
        assertThat(dce.getResponseHeaders()).isEmpty();
    }

    @Test(groups = { "unit" })
    public void headerNotNull5() {
        DocumentClientException dce = new DocumentClientException((String) null, 0, (Error) null, (Map) null);
        assertThat(dce.getResponseHeaders()).isNotNull();
        assertThat(dce.getResponseHeaders()).isEmpty();
    }

    @Test(groups = { "unit" })
    public void headerNotNull6() {
        DocumentClientException dce = new DocumentClientException((String) null, (Exception) null, (Map) null, 0, (String) null);
        assertThat(dce.getResponseHeaders()).isNotNull();
        assertThat(dce.getResponseHeaders()).isEmpty();
    }

    @Test(groups = { "unit" })
    public void headerNotNull7() {
        ImmutableMap<String, String> respHeaders = ImmutableMap.of("key", "value");
        DocumentClientException dce = new DocumentClientException((String) null, (Exception) null, respHeaders, 0, (String) null);
        assertThat(dce.getResponseHeaders()).isNotNull();
        assertThat(dce.getResponseHeaders()).contains(respHeaders.entrySet().iterator().next());
    }

    @Test(groups = { "unit" })
    public void nullClearsRequestHeaders(Method method) {

        final DocumentClientException dce = new DocumentClientException(0, method.getName());
        final Map<String, String> values = new HashMap<>();

        values.put("foo", "bar");
        values.put("bar", "baz");

        assertNotNull(dce.getRequestHeaders());

        try {
            dce.setRequestHeaders(values);
        } catch (Throwable error) {
            fail(lenientFormat("unexpected %s", error), error);
            return;
        }

        assertThat(dce.getRequestHeaders().size()).isEqualTo(values.size());

        try {
            dce.setRequestHeaders(null);
        } catch (Throwable error) {
            fail(lenientFormat("unexpected %s", error), error);
            return;
        }

        assertThat(dce.getRequestHeaders().size()).isZero();
    }

    @Test(groups = { "unit" })
    public void nullValuesInRequestHeadersAreIgnored(Method method) {

        final DocumentClientException dce = new DocumentClientException(0, method.getName());
        final Map<String, String> values = new HashMap<>();
        values.put("foo", null);

        assertNotNull(dce.getRequestHeaders());

        try {
            dce.setRequestHeaders(values);
        } catch (Throwable error) {
            fail(lenientFormat("unexpected %s", error), error);
        }

        assertThat(dce.getRequestHeaders()).isEmpty();
    }

    @Test(groups = { "unit" }, dataProvider = "subTypes")
    public void statusCodeIsCorrect(Class<DocumentClientException> type, int expectedStatusCode) {
        try {
            final DocumentClientException instance = type
                .getConstructor(String.class,  HttpResponseHeaders.class, String.class)
                .newInstance("some-message", null, "some-uri");
            assertEquals(instance.getStatusCode(), expectedStatusCode);
        } catch (IllegalAccessException | InstantiationException | NoSuchMethodException | InvocationTargetException error) {
            String message = lenientFormat("could not create instance of %s due to %s", type, error);
            throw new AssertionError(message, error);
        }
    }

    @DataProvider(name = "subTypes")
    private static Object[][] subTypes() {
        return new Object[][] {
            { BadRequestException.class, BADREQUEST },
            { GoneException.class, GONE },
            { InternalServerErrorException.class, INTERNAL_SERVER_ERROR },
            { RequestTimeoutException.class, REQUEST_TIMEOUT },
        };
    }
}
