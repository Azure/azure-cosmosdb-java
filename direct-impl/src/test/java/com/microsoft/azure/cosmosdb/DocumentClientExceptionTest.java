/*
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

package com.microsoft.azure.cosmosdb;

import com.google.common.collect.ImmutableMap;
import com.microsoft.azure.cosmosdb.internal.InternalServerErrorException;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.ConflictException;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.ForbiddenException;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.GoneException;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.LockedException;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.MethodNotAllowedException;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.PartitionKeyRangeGoneException;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.PreconditionFailedException;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.RequestEntityTooLargeException;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.RequestRateTooLargeException;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.RequestTimeoutException;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.RetryWithException;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.ServiceUnavailableException;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.UnauthorizedException;
import com.microsoft.azure.cosmosdb.rx.internal.BadRequestException;
import com.microsoft.azure.cosmosdb.rx.internal.InvalidPartitionException;
import com.microsoft.azure.cosmosdb.rx.internal.NotFoundException;
import com.microsoft.azure.cosmosdb.rx.internal.PartitionIsMigratingException;
import com.microsoft.azure.cosmosdb.rx.internal.PartitionKeyRangeIsSplittingException;
import io.reactivex.netty.protocol.http.client.HttpResponseHeaders;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import static com.google.common.base.Strings.lenientFormat;
import static com.microsoft.azure.cosmosdb.internal.HttpConstants.StatusCodes.BADREQUEST;
import static com.microsoft.azure.cosmosdb.internal.HttpConstants.StatusCodes.CONFLICT;
import static com.microsoft.azure.cosmosdb.internal.HttpConstants.StatusCodes.FORBIDDEN;
import static com.microsoft.azure.cosmosdb.internal.HttpConstants.StatusCodes.GONE;
import static com.microsoft.azure.cosmosdb.internal.HttpConstants.StatusCodes.INTERNAL_SERVER_ERROR;
import static com.microsoft.azure.cosmosdb.internal.HttpConstants.StatusCodes.LOCKED;
import static com.microsoft.azure.cosmosdb.internal.HttpConstants.StatusCodes.METHOD_NOT_ALLOWED;
import static com.microsoft.azure.cosmosdb.internal.HttpConstants.StatusCodes.NOTFOUND;
import static com.microsoft.azure.cosmosdb.internal.HttpConstants.StatusCodes.PRECONDITION_FAILED;
import static com.microsoft.azure.cosmosdb.internal.HttpConstants.StatusCodes.REQUEST_ENTITY_TOO_LARGE;
import static com.microsoft.azure.cosmosdb.internal.HttpConstants.StatusCodes.REQUEST_TIMEOUT;
import static com.microsoft.azure.cosmosdb.internal.HttpConstants.StatusCodes.RETRY_WITH;
import static com.microsoft.azure.cosmosdb.internal.HttpConstants.StatusCodes.SERVICE_UNAVAILABLE;
import static com.microsoft.azure.cosmosdb.internal.HttpConstants.StatusCodes.TOO_MANY_REQUESTS;
import static com.microsoft.azure.cosmosdb.internal.HttpConstants.StatusCodes.UNAUTHORIZED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class DocumentClientExceptionTest {

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
            { ConflictException.class, CONFLICT },
            { ForbiddenException.class, FORBIDDEN },
            { GoneException.class, GONE },
            { InternalServerErrorException.class, INTERNAL_SERVER_ERROR },
            { InvalidPartitionException.class, GONE },
            { LockedException.class, LOCKED },
            { MethodNotAllowedException.class, METHOD_NOT_ALLOWED },
            { NotFoundException.class, NOTFOUND },
            { PartitionIsMigratingException.class, GONE },
            { PartitionKeyRangeGoneException.class, GONE },
            { PartitionKeyRangeIsSplittingException.class, GONE },
            { PreconditionFailedException.class, PRECONDITION_FAILED },
            { RequestEntityTooLargeException.class, REQUEST_ENTITY_TOO_LARGE },
            { RequestRateTooLargeException.class, TOO_MANY_REQUESTS },
            { RequestTimeoutException.class, REQUEST_TIMEOUT },
            { RetryWithException.class, RETRY_WITH },
            { ServiceUnavailableException.class, SERVICE_UNAVAILABLE },
            { UnauthorizedException.class, UNAUTHORIZED }
        };
    }
}
