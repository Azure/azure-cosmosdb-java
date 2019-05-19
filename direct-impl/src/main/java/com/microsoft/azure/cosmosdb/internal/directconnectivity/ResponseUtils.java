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

import com.microsoft.azure.cosmosdb.rx.internal.http.HttpHeaders;
import com.microsoft.azure.cosmosdb.rx.internal.http.HttpResponse;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

class ResponseUtils {
    private final static int INITIAL_RESPONSE_BUFFER_SIZE = 1024;
    private final static Logger logger = LoggerFactory.getLogger(ResponseUtils.class);

    public static Flux<String> toString(Flux<ByteBuf> contentObservable) {
        return contentObservable
                .reduce(
                        new ByteArrayOutputStream(INITIAL_RESPONSE_BUFFER_SIZE),
                        (out, bb) -> {
                            try {
                                bb.readBytes(out, bb.readableBytes());
                                return out;
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        })
                .map(out -> new String(out.toByteArray(), StandardCharsets.UTF_8)).flux();
    }

    static Mono<StoreResponse> toStoreResponse(HttpResponse httpClientResponse, Flux<ByteBuf> byteBufFlux) {

        HttpHeaders httpResponseHeaders = httpClientResponse.headers();

        Flux<String> contentObservable;

        if (byteBufFlux == null) {
            // for delete we don't expect any body
            contentObservable = Flux.empty();
        } else {
            // transforms the ByteBufFlux to Flux<String>
            contentObservable = toString(byteBufFlux);
        }

        Flux<StoreResponse> storeResponseFlux = contentObservable.flatMap(content -> {
            try {
                // transforms to Observable<StoreResponse>
                StoreResponse rsp = new StoreResponse(httpClientResponse.statusCode(), HttpUtils.unescape(httpResponseHeaders.toMap().entrySet()), content);
                return Flux.just(rsp);
            } catch (Exception e) {
                return Flux.error(e);
            }
        });

        return storeResponseFlux.single();
    }
}
