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
package com.microsoft.azure.cosmosdb.rx.internal.query;

import com.microsoft.azure.cosmosdb.BridgeInternal;
import com.microsoft.azure.cosmosdb.DocumentClientException;
import com.microsoft.azure.cosmosdb.FeedResponse;
import com.microsoft.azure.cosmosdb.Resource;
import com.microsoft.azure.cosmosdb.internal.HttpConstants;
import com.microsoft.azure.cosmosdb.rx.internal.BadRequestException;
import com.microsoft.azure.cosmosdb.rx.internal.Utils;
import rx.Observable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class DistinctDocumentQueryExecutionContext<T extends Resource> implements IDocumentQueryExecutionComponent<T> {
    private final IDocumentQueryExecutionComponent<T> component;
    private final DistinctMap distinctMap;
    private final DistinctQueryType distinctQueryType;
    private String lastHash;

    public DistinctDocumentQueryExecutionContext(IDocumentQueryExecutionComponent<T> component,
                                                 DistinctQueryType distinctQueryType,
                                                 String previousHash) {
        if (distinctQueryType == DistinctQueryType.None) {
            throw new IllegalArgumentException("Invalid distinct query type");
        }

        if (component == null) {
            throw new IllegalArgumentException("documentQueryExecutionComponent cannot be null");
        }

        this.distinctQueryType = distinctQueryType;
        this.component = component;
        this.distinctMap = DistinctMap.create(distinctQueryType, previousHash);
    }

    public static <T extends Resource> Observable<IDocumentQueryExecutionComponent<T>> createAsync(
            Function<String, Observable<IDocumentQueryExecutionComponent<T>>> createSourceComponentFunction,
            DistinctQueryType distinctQueryType,
            String continuationToken) {

        if (distinctQueryType == DistinctQueryType.Ordered) {
            DocumentClientException dce = new BadRequestException("Ordered distinct queries are not supported yet");
            return Observable.error(dce);
        }

        Utils.ValueHolder<DistinctContinuationToken> outDistinctcontinuationtoken = new Utils.ValueHolder<>();
        DistinctContinuationToken distinctContinuationToken = new DistinctContinuationToken(null, null);

        if (continuationToken != null) {
            if (!DistinctContinuationToken.tryParse(continuationToken, outDistinctcontinuationtoken)) {
                return Observable.error(new BadRequestException("Invalid DistinctContinuationToken" + continuationToken));
            } else {
                distinctContinuationToken = outDistinctcontinuationtoken.v;
                if (distinctQueryType != DistinctQueryType.Ordered && distinctContinuationToken.getLastHash() != null) {
                    DocumentClientException dce = new BadRequestException("DistinctContinuationToken is malformed." +
                                                                                  " DistinctContinuationToken can not" +
                                                                                  " have a 'lastHash', when the query" +
                                                                                  " type is not ordered (ex SELECT " +
                                                                                  "DISTINCT VALUE c.blah FROM c ORDER" +
                                                                                  " BY c.blah).");
                    return Observable.error(dce);
                }
            }
        }

        final String continuationTokenLastHash = distinctContinuationToken.getLastHash();

        return createSourceComponentFunction.apply(distinctContinuationToken.getSourceToken()).map(component -> {
            return new DistinctDocumentQueryExecutionContext<T>(component,
                    distinctQueryType,
                    continuationTokenLastHash);
        });
    }

    IDocumentQueryExecutionComponent<T> getComponent() {
        return this.component;
    }

    @Override
    public Observable<FeedResponse<T>> drainAsync(int maxPageSize) {
        return this.component.drainAsync(maxPageSize).map(tFeedResponse -> {

            final List<T> distinctResults = new ArrayList<>();

            tFeedResponse.getResults().forEach(document -> {
                Utils.ValueHolder<String> outHash = new Utils.ValueHolder<>();
                if (this.distinctMap.add(document, outHash)) { // this.distinctMap.Add(document, out this.lastHash)
                    System.out.println("document = " + document + "outhash: " + outHash);
                    distinctResults.add(document);
                    lastHash = outHash.v;
                }
            });

            Map<String, String> headers = new HashMap<>(tFeedResponse.getResponseHeaders());
            if (tFeedResponse.getResponseContinuation() != null) {

                final String sourceContinuationToken = tFeedResponse.getResponseContinuation();
                final DistinctContinuationToken distinctContinuationToken = new DistinctContinuationToken(this.lastHash,
                        sourceContinuationToken);
                System.out.println("distinctContinuationToken.toJson() = " + distinctContinuationToken.toJson());
                headers.put(HttpConstants.HttpHeaders.CONTINUATION, distinctContinuationToken.toJson());
            }

            return BridgeInternal.createFeedResponseWithQueryMetrics(distinctResults, headers,
                    tFeedResponse.getQueryMetrics());
        });

    }
}
