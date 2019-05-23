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

import com.microsoft.azure.cosmosdb.DocumentClientException;
import com.microsoft.azure.cosmosdb.Error;
import com.microsoft.azure.cosmosdb.internal.HttpConstants;
import com.microsoft.azure.cosmosdb.rx.internal.RxDocumentServiceResponse;
import com.microsoft.azure.cosmosdb.rx.internal.http.HttpResponse;
import hu.akarnokd.rxjava.interop.RxJavaInterop;
import reactor.adapter.rxjava.RxJava2Adapter;
import reactor.core.publisher.Mono;
import rx.Single;

public class HttpClientUtils {

    static Single<RxDocumentServiceResponse> parseResponseAsync(Mono<HttpResponse> httpResponse) {
        return RxJavaInterop.toV1Single(RxJava2Adapter.monoToSingle(httpResponse.flatMap(response -> {
            if (response.statusCode() < HttpConstants.StatusCodes.MINIMUM_STATUSCODE_AS_ERROR_GATEWAY) {

                return ResponseUtils.toStoreResponse(response).map(RxDocumentServiceResponse::new);

                // TODO: to break the dependency between RxDocumentServiceResponse and StoreResponse
                // we should factor out the  RxDocumentServiceResponse(StoreResponse) constructor to a helper class

            } else {
                return HttpClientUtils
                        .createDocumentClientException(response).flatMap(Mono::error);
            }
        })));
    }

    private static Mono<DocumentClientException> createDocumentClientException(HttpResponse httpResponse) {
        Mono<String> readStream = httpResponse.bodyAsString();

        return readStream.map(body -> {
            Error error = new Error(body);

            // TODO: we should set resource address in the Document Client Exception
            return new DocumentClientException(httpResponse.statusCode(), error,
                    httpResponse.headers().toMap());
        });
    }
}
