package com.microsoft.azure.cosmosdb.rx.internal.query;

import com.microsoft.azure.cosmosdb.SqlQuerySpec;
import com.microsoft.azure.cosmosdb.internal.HttpConstants;
import com.microsoft.azure.cosmosdb.internal.OperationType;
import com.microsoft.azure.cosmosdb.internal.ResourceType;
import com.microsoft.azure.cosmosdb.internal.RuntimeConstants;
import com.microsoft.azure.cosmosdb.internal.query.PartitionedQueryExecutionInfo;
import com.microsoft.azure.cosmosdb.rx.internal.RxDocumentServiceRequest;
import rx.Single;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

class QueryPlanRetriever {
    private static final String TRUE = "True";
    private static final String QUERY_VERSION = "1.0";
    private static final String supportedQueryFeatures = QueryFeature.Aggregate.name() + ", " +
            QueryFeature.CompositeAggregate.name() + "," +
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
        requestHeaders.put(HttpConstants.HttpHeaders.SUPPORTED_QUERY_FEATURES, supportedQueryFeatures);
        requestHeaders.put(HttpConstants.HttpHeaders.QUERY_VERSION, QUERY_VERSION);

        RxDocumentServiceRequest request;
        request = RxDocumentServiceRequest.create(OperationType.QueryPlan,
                ResourceType.Document,
                resourceLink,
                requestHeaders);
        request.UseGatewayMode = true;
        request.setContentBytes(sqlQuerySpec.toJson().getBytes(StandardCharsets.UTF_8));

        return queryClient.executeQueryAsync(request).flatMap(rxDocumentServiceResponse -> {
            PartitionedQueryExecutionInfo partitionedQueryExecutionInfo =
                    new PartitionedQueryExecutionInfo(rxDocumentServiceResponse.getReponseBodyAsString());
            return Single.just(partitionedQueryExecutionInfo);
        });
    }
}
