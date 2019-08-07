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

import com.microsoft.azure.cosmosdb.SqlQuerySpec;
import com.microsoft.azure.cosmosdb.internal.HttpConstants;
import com.microsoft.azure.cosmosdb.internal.OperationType;
import com.microsoft.azure.cosmosdb.internal.ResourceType;
import com.microsoft.azure.cosmosdb.internal.RuntimeConstants;
import com.microsoft.azure.cosmosdb.internal.query.PartitionedQueryExecutionInfo;
import com.microsoft.azure.cosmosdb.rx.internal.BackoffRetryUtility;
import com.microsoft.azure.cosmosdb.rx.internal.IDocumentClientRetryPolicy;
import com.microsoft.azure.cosmosdb.rx.internal.RxDocumentServiceRequest;
import rx.Single;
import rx.functions.Func1;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

class QueryPlanRetriever {
    private static final String TRUE = "True";
    private static final String SUPPORTED_QUERY_FEATURES = QueryFeature.Aggregate.name() + ", " +
            QueryFeature.CompositeAggregate.name() + ", " +
            QueryFeature.Distinct.name() + ", " +
            QueryFeature.MultipleOrderBy.name() + ", " +
            QueryFeature.OffsetAndLimit.name() + ", " +
            QueryFeature.OrderBy.name() + ", " +
            QueryFeature.Top.name();

    static Single<PartitionedQueryExecutionInfo> getQueryPlanThroughGatewayAsync(IDocumentQueryClient queryClient,
                                                                                 SqlQuerySpec sqlQuerySpec, String resourceLink) {
        Map<String, String> requestHeaders = new HashMap<>();
        requestHeaders.put(HttpConstants.HttpHeaders.CONTENT_TYPE, RuntimeConstants.MediaTypes.JSON);
        requestHeaders.put(HttpConstants.HttpHeaders.IS_QUERY_PLAN_REQUEST, TRUE);
        requestHeaders.put(HttpConstants.HttpHeaders.SUPPORTED_QUERY_FEATURES, SUPPORTED_QUERY_FEATURES);
        requestHeaders.put(HttpConstants.HttpHeaders.QUERY_VERSION, HttpConstants.Versions.QUERY_VERSION);

        RxDocumentServiceRequest request;
        request = RxDocumentServiceRequest.create(OperationType.QueryPlan,
                ResourceType.Document,
                resourceLink,
                requestHeaders);
        request.UseGatewayMode = true;
        request.setContentBytes(sqlQuerySpec.toJson().getBytes(StandardCharsets.UTF_8));

        final IDocumentClientRetryPolicy retryPolicyInstance = queryClient.getResetSessionTokenRetryPolicy().getRequestPolicy();

        Func1<RxDocumentServiceRequest, Single<PartitionedQueryExecutionInfo>> executeFunc = req -> {
            return BackoffRetryUtility.executeRetry(() -> {
                retryPolicyInstance.onBeforeSendRequest(req);
                return queryClient.executeQueryAsync(request).flatMap(rxDocumentServiceResponse -> {
                    PartitionedQueryExecutionInfo partitionedQueryExecutionInfo =
                            new PartitionedQueryExecutionInfo(rxDocumentServiceResponse.getReponseBodyAsString());
                    return Single.just(partitionedQueryExecutionInfo);

                });
            }, retryPolicyInstance);
        };

        return executeFunc.call(request);
    }
}
