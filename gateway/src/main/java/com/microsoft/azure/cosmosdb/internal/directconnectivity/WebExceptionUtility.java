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

import com.microsoft.azure.cosmosdb.rx.internal.Utils;
import io.netty.channel.ChannelException;
import io.reactivex.netty.client.PoolExhaustedException;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLPeerUnverifiedException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.HttpRetryException;
import java.net.NoRouteToHostException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.net.UnknownServiceException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.InterruptedByTimeoutException;

public class WebExceptionUtility {
    public static boolean isWebExceptionRetriable(Exception ex) {
        Exception iterator = ex;

        while (iterator != null) {
            if (WebExceptionUtility.isWebExceptionRetriableInternal(iterator)) {
                return true;
            }

            Throwable t = iterator.getCause();
            iterator = Utils.as(t, Exception.class);
        }

        return false;
    }

    private static boolean isWebExceptionRetriableInternal(Exception ex) {
        if (ex instanceof PoolExhaustedException) {
            return true;
        }

        IOException webEx = Utils.as(ex, IOException.class);
        if (webEx == null) {
            return false;
        }

        // any network failure for which we are certain the request hasn't reached the service endpoint.
        if (webEx instanceof ConnectException ||
                webEx instanceof UnknownHostException ||
                webEx instanceof SSLHandshakeException ||
                webEx instanceof NoRouteToHostException ||
                webEx instanceof SSLPeerUnverifiedException) {
            return true;
        }

        return false;
    }

    public static boolean isNetworkFailure(Exception ex) {
        Exception iterator = ex;

        while (iterator != null) {
            if (WebExceptionUtility.isNetworkFailureInternal(iterator)) {
                return true;
            }

            Throwable t = iterator.getCause();
            iterator = Utils.as(t, Exception.class);
        }

        return false;
    }

    private static boolean isNetworkFailureInternal(Exception ex) {
        //  We have seen these cases in CRIs
        if (ex instanceof ClosedChannelException
            || ex instanceof SocketException
            || ex instanceof SSLException
            || ex instanceof UnknownHostException) {
            return true;
        }

        //  These cases might be related, but we haven't seen them ever
        if (ex instanceof UnknownServiceException
            || ex instanceof HttpRetryException
            || ex instanceof InterruptedByTimeoutException
            || ex instanceof InterruptedIOException) {
            return true;
        }

        if (ex instanceof ChannelException) {
            return true;
        }

        return false;
    }
}
