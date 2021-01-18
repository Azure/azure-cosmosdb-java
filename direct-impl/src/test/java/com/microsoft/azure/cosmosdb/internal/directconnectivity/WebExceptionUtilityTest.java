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

package com.microsoft.azure.cosmosdb.internal.directconnectivity;

import com.fasterxml.jackson.databind.JsonMappingException;
import io.netty.channel.ChannelException;
import io.netty.channel.ConnectTimeoutException;
import io.netty.handler.timeout.ReadTimeoutException;
import io.reactivex.netty.client.PoolExhaustedException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLPeerUnverifiedException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.HttpRetryException;
import java.net.NoRouteToHostException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.net.UnknownServiceException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.InterruptedByTimeoutException;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * validation tests for {@link WebExceptionUtility}
 */
public class WebExceptionUtilityTest {

    @DataProvider(name = "exceptionToIsRetriable")
    public Object[][] exceptionToIsRetriable() {
        return new Object[][]{
                // exception, is retriable
                {
                        new RuntimeException(), false
                },
                {
                        new ConnectException(), true
                },
                {
                        new ConnectTimeoutException(), true
                },
                {
                        new UnknownHostException(), true
                },
                {
                        ReadTimeoutException.INSTANCE, false
                },
                {
                        new SSLHandshakeException("dummy"), true
                },
                {
                        new NoRouteToHostException(), true,
                },
                {
                        new SSLPeerUnverifiedException("dummy"), true
                },
                {
                        new SocketTimeoutException(), false
                },
                {
                        new PoolExhaustedException(), true
                }
        };
    }

    @Test(groups = "unit", dataProvider = "exceptionToIsRetriable")
    public void isWebExceptionRetriable(Exception e, boolean isRetriable) {
        boolean actualRes = WebExceptionUtility.isWebExceptionRetriable(e);
        if (isRetriable) {
            assertThat(actualRes).describedAs(e.toString()).isTrue();
        } else {
            assertThat(actualRes).describedAs(e.toString()).isFalse();
        }
    }

    @DataProvider(name = "networkFailure")
    public Object[][] networkFailure() {
        return new Object[][]{
                // exception, is retriable
                {
                        new RuntimeException(), false
                },
                {
                        new ConnectException(), true
                },
                {
                        new ConnectTimeoutException(), true
                },
                {
                        new UnknownHostException(), true
                },
                {
                        ReadTimeoutException.INSTANCE, true
                },
                {
                        new SSLHandshakeException("dummy"), true
                },
                {
                        new NoRouteToHostException(), true,
                },
                {
                        new SSLPeerUnverifiedException("dummy"), true
                },
                {
                        new SocketTimeoutException(), true
                },
                {
                        new ChannelException(), true
                },
                {
                        new IOException(), false
                },
                {
                        new ClosedChannelException(), true
                },
                {
                        new SocketException(), true
                },
                {
                        new SSLException("dummy"), true
                },
                {
                        new UnknownServiceException(), true
                },
                {
                        new HttpRetryException("dummy", 500), true
                },
                {
                        new InterruptedByTimeoutException(), true
                },
                {
                        new InterruptedIOException(), true
                },
                {
                        new JsonMappingException(null, "dummy"), false
                }
        };
    }

    @Test(groups = "unit", dataProvider = "networkFailure")
    public void isNetworkFailure(Exception e, boolean isNetworkFailure) {
        boolean actualRes = WebExceptionUtility.isNetworkFailure(e);
        if (isNetworkFailure) {
            assertThat(actualRes).describedAs(e.toString()).isTrue();
        } else {
            assertThat(actualRes).describedAs(e.toString()).isFalse();
        }
    }
}
