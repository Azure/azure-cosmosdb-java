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

import java.util.List;
import java.util.UUID;

import com.microsoft.azure.cosmosdb.DocumentClientException;
import com.microsoft.azure.cosmosdb.DocumentCollection;
import com.microsoft.azure.cosmosdb.FeedOptions;
import com.microsoft.azure.cosmosdb.PartitionKeyRange;
import com.microsoft.azure.cosmosdb.Resource;
import com.microsoft.azure.cosmosdb.SqlQuerySpec;
import com.microsoft.azure.cosmosdb.internal.HttpConstants;
import com.microsoft.azure.cosmosdb.internal.OperationType;
import com.microsoft.azure.cosmosdb.internal.ResourceType;
import com.microsoft.azure.cosmosdb.internal.query.PartitionedQueryExecutionInfo;
import com.microsoft.azure.cosmosdb.internal.query.QueryInfo;
import com.microsoft.azure.cosmosdb.rx.internal.BadRequestException;
import com.microsoft.azure.cosmosdb.rx.internal.RxDocumentServiceRequest;
import com.microsoft.azure.cosmosdb.rx.internal.Strings;
import com.microsoft.azure.cosmosdb.rx.internal.Utils;
import com.microsoft.azure.cosmosdb.rx.internal.caches.RxCollectionCache;

import rx.Observable;
import rx.Single;

/**
 * While this class is public, but it is not part of our published public APIs.
 * This is meant to be internally used only by our sdk.
 */
public class DocumentQueryExecutionContextFactory {

    private final static int PageSizeFactorForTop = 5;

    private static Single<DocumentCollection> resolveCollection(IDocumentQueryClient client, SqlQuerySpec query, 
            ResourceType resourceTypeEnum, String resourceLink) {

        RxCollectionCache collectionCache = client.getCollectionCache();

        RxDocumentServiceRequest request = RxDocumentServiceRequest.create(
                OperationType.Query,
                resourceTypeEnum,
                resourceLink, null
                // TODO      AuthorizationTokenType.Invalid)
                ); //this request doesnt actually go to server
        return collectionCache.resolveCollectionAsync(request);
    }

    public static <T extends Resource> Observable<? extends IDocumentQueryExecutionContext<T>> createDocumentQueryExecutionContextAsync(
            IDocumentQueryClient client,
            ResourceType resourceTypeEnum,
            Class<T> resourceType,
            SqlQuerySpec query,
            FeedOptions feedOptions,
            String resourceLink,
            boolean isContinuationExpected,
            UUID correlatedActivityId) {

        Observable<DocumentCollection> collectionObs = Observable.just(null);
        
        if (resourceTypeEnum.isCollectionChild()) {
            collectionObs = resolveCollection(client, query, resourceTypeEnum, resourceLink).toObservable();
        }

        DefaultDocumentQueryExecutionContext<T> queryExecutionContext =  new DefaultDocumentQueryExecutionContext<T>(
                client,
                resourceTypeEnum,
                resourceType,
                query,
                feedOptions,
                resourceLink,
                correlatedActivityId,
                isContinuationExpected);

        if (ResourceType.Document != resourceTypeEnum
                || (feedOptions != null && feedOptions.getPartitionKeyRangeIdInternal() != null)) {
            return Observable.just(queryExecutionContext);
        }

        Single<PartitionedQueryExecutionInfo> queryExecutionInfoSingle =
                QueryPlanRetriever.getQueryPlanThroughGatewayAsync(client, query, resourceLink);

        return collectionObs.flatMap(collection -> queryExecutionInfoSingle.toObservable()
                .flatMap(partitionedQueryExecutionInfo -> {
                    QueryInfo queryInfo = partitionedQueryExecutionInfo.getQueryInfo();
                    
                    // Non value aggregates must go through DefaultDocumentQueryExecutionContext
                    // Single partition query can serve queries like SELECT AVG(c.age) FROM c
                    // SELECT MIN(c.age) + 5 FROM c
                    // SELECT MIN(c.age), MAX(c.age) FROM c
                    // while pipelined queries can only serve
                    // SELECT VALUE <AGGREGATE>. So we send the query down the old pipeline to avoid a breaking change.
                    // We will skip this in V3 SDK
                    if(queryInfo.hasAggregates() && !queryInfo.hasSelectValue()){
                        if(feedOptions != null && feedOptions.getEnableCrossPartitionQuery()){
                            return Observable.error(new DocumentClientException(HttpConstants.StatusCodes.BADREQUEST,
                                    "Cross partition query only supports 'VALUE <AggreateFunc>' for aggregates"));
                        }
                        return Observable.just( queryExecutionContext);
                    }
                    
                    Single<List<PartitionKeyRange>> partitionKeyRanges = queryExecutionContext
                            .getTargetPartitionKeyRanges(collection.getResourceId(), partitionedQueryExecutionInfo.getQueryRanges());

                    Observable<IDocumentQueryExecutionContext<T>> exContext = partitionKeyRanges
                            .toObservable()
                            .flatMap(pkranges -> createSpecializedDocumentQueryExecutionContextAsync(client,
                                                                                                     resourceTypeEnum,
                                                                                                     resourceType,
                                                                                                     query,
                                                                                                     feedOptions,
                                                                                                     resourceLink,
                                                                                                     isContinuationExpected,
                                                                                                     partitionedQueryExecutionInfo,
                                                                                                     pkranges,
                                                                                                     collection.getResourceId(),
                                                                                                     correlatedActivityId));

                    return exContext;
                }));
    }

	public static <T extends Resource> Observable<? extends IDocumentQueryExecutionContext<T>> createSpecializedDocumentQueryExecutionContextAsync(
            IDocumentQueryClient client,
            ResourceType resourceTypeEnum,
            Class<T> resourceType,
            SqlQuerySpec query,
            FeedOptions feedOptions,
            String resourceLink,
            boolean isContinuationExpected,
            PartitionedQueryExecutionInfo partitionedQueryExecutionInfo,
            List<PartitionKeyRange> targetRanges,
            String collectionRid,
            UUID correlatedActivityId) {

        if (feedOptions == null) {
            feedOptions = new FeedOptions();
        }

        int initialPageSize = Utils.getValueOrDefault(feedOptions.getMaxItemCount(),
                                                      ParallelQueryConfig.ClientInternalPageSize);

        BadRequestException validationError = Utils.checkRequestOrReturnException
                (initialPageSize > 0, "MaxItemCount", "Invalid MaxItemCount %s", initialPageSize);
        if (validationError != null) {
            return Observable.error(validationError);
        }

        QueryInfo queryInfo = partitionedQueryExecutionInfo.getQueryInfo();
        
        if (!Strings.isNullOrEmpty(queryInfo.getRewrittenQuery())) {
                query = new SqlQuerySpec(queryInfo.getRewrittenQuery(), query.getParameters());
        }

        boolean getLazyFeedResponse = queryInfo.hasTop();

        // We need to compute the optimal initial page size for order-by queries
        if (queryInfo.hasOrderBy()) {
            int top;
            if (queryInfo.hasTop() && (top = partitionedQueryExecutionInfo.getQueryInfo().getTop()) > 0) {
                int pageSizeWithTop = Math.min(
                        (int)Math.ceil(top / (double)targetRanges.size()) * PageSizeFactorForTop,
                        top);

                if (initialPageSize > 0) {
                    initialPageSize = Math.min(pageSizeWithTop, initialPageSize);
                }
                else {
                    initialPageSize = pageSizeWithTop;
                }
            }
            // TODO: do not support continuation in string format right now
            //            else if (isContinuationExpected)
            //            {
            //                if (initialPageSize < 0)
            //                {
            //                    initialPageSize = (int)Math.Max(feedOptions.MaxBufferedItemCount, ParallelQueryConfig.GetConfig().DefaultMaximumBufferSize);
            //                }
            //
            //                initialPageSize = Math.Min(
            //                    (int)Math.Ceiling(initialPageSize / (double)targetRanges.Count) * PageSizeFactorForTop,
            //                    initialPageSize);
            //            } 
        }

        return PipelinedDocumentQueryExecutionContext.createAsync(
                client,
                resourceTypeEnum,
                resourceType,
                query,
                feedOptions,
                resourceLink,
                collectionRid,
                partitionedQueryExecutionInfo,
                targetRanges,
                initialPageSize,
                isContinuationExpected,
                getLazyFeedResponse,
                correlatedActivityId);           
    }
}
