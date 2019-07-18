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

import com.microsoft.azure.cosmosdb.internal.ByteBufferPool;
import com.microsoft.azure.cosmosdb.internal.HttpConstants;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import io.reactivex.netty.protocol.http.client.HttpResponseHeaders;
import rx.Observable;
import rx.Single;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

class ResponseUtils {
    public static Observable<String> toString(Observable<ByteBuf> contentObservable, final int contentLength) {
        if (contentLength <= 0) {
            return Observable.just("");
        }

        ByteBufferPool.ByteBufferWrapper byteBufferWrapper = ByteBufferPool.getInstant().lease(contentLength);

        return contentObservable
                .reduce(
                        byteBufferWrapper,
                        (out, bb) -> {
                            try {
                                int limit = bb.readableBytes();
                                out.getByteBuffer().limit(limit);
                                bb.readBytes(out.getByteBuffer());
                                assert contentLength == limit;

                                return out;
                            } catch (Throwable t) {
                                ByteBufferPool.getInstant().release(byteBufferWrapper);
                                throw new RuntimeException(t);
                            }
                        })
                .map(out -> {
                    try {
                        out.getByteBuffer().position(0);
                        return new String(out.getByteBuffer().array(), 0, contentLength, StandardCharsets.UTF_8);
                    } finally {
                        ByteBufferPool.getInstant().release(byteBufferWrapper);
                    }
                });
    }

    public static Single<StoreResponse> toStoreResponse(HttpClientResponse<ByteBuf> clientResponse) {

        HttpResponseHeaders httpResponseHeaders = clientResponse.getHeaders();
        HttpResponseStatus httpResponseStatus = clientResponse.getStatus();

        Observable<String> contentObservable;

        if (clientResponse.getContent() == null) {
            // for delete we don't expect any body
            contentObservable = Observable.just(null);
        } else {
            // transforms the observable<ByteBuf> to Observable<InputStream>
            contentObservable = toString(clientResponse.getContent(), clientResponse.getHeaders().getIntHeader(HttpConstants.HttpHeaders.CONTENT_LENGTH, -1));
        }

        Observable<StoreResponse> storeResponseObservable = contentObservable
                .flatMap(content -> {
                    try {
                        // transforms to Observable<StoreResponse>
                        StoreResponse rsp = new StoreResponse(httpResponseStatus.code(), HttpUtils.unescape(httpResponseHeaders.entries()), content);
                        return Observable.just(rsp);
                    } catch (Exception e) {
                        return Observable.error(e);
                    }
                });

        return storeResponseObservable.toSingle();
    }
}
