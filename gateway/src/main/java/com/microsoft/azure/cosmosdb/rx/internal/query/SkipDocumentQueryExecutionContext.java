package com.microsoft.azure.cosmosdb.rx.internal.query;

import com.microsoft.azure.cosmosdb.BridgeInternal;
import com.microsoft.azure.cosmosdb.DocumentClientException;
import com.microsoft.azure.cosmosdb.FeedResponse;
import com.microsoft.azure.cosmosdb.Resource;
import com.microsoft.azure.cosmosdb.internal.HttpConstants;
import com.microsoft.azure.cosmosdb.rx.internal.Utils;
import rx.Observable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class SkipDocumentQueryExecutionContext<T extends Resource> implements IDocumentQueryExecutionComponent<T> {

    private final IDocumentQueryExecutionComponent<T> component;
    private int skipCount;

    SkipDocumentQueryExecutionContext(IDocumentQueryExecutionComponent<T> component, int skipCount) {
        this.component = component;
        this.skipCount = skipCount;
    }

    public static <T extends Resource> Observable<IDocumentQueryExecutionComponent<T>> createAsync(Function<String,
            Observable<IDocumentQueryExecutionComponent<T>>> createSourceComponentFunction, int skipCount,
                                                                                                   String continuationToken) {
        OffsetContinuationToken offsetContinuationToken;
        Utils.ValueHolder<OffsetContinuationToken> outOffsetContinuationToken = new Utils.ValueHolder<>();

        if (continuationToken != null) {
            if (!OffsetContinuationToken.tryParse(continuationToken, outOffsetContinuationToken)) {
                String message = String.format("Invalid JSON in continuation token %s for Parallel~Context",
                        continuationToken);
                DocumentClientException dce = new DocumentClientException(HttpConstants.StatusCodes.BADREQUEST,
                        message);
                return Observable.error(dce);
            }

            offsetContinuationToken = outOffsetContinuationToken.v;
        } else {
            offsetContinuationToken = new OffsetContinuationToken(skipCount, null);
        }

        return createSourceComponentFunction.apply(offsetContinuationToken.getSourceToken())
                .map(component -> new SkipDocumentQueryExecutionContext<>(component, offsetContinuationToken.getOffset()));
    }

    @Override
    public Observable<FeedResponse<T>> drainAsync(int maxPageSize) {

        return this.component.drainAsync(maxPageSize).map(tFeedResponse -> {

            List<T> documentsAfterSkip =
                    tFeedResponse.getResults().stream().skip(this.skipCount).collect(Collectors.toList());

            int numberOfDocumentsSkipped = tFeedResponse.getResults().size() - documentsAfterSkip.size();
            this.skipCount -= numberOfDocumentsSkipped;

            Map<String, String> headers = new HashMap<>(tFeedResponse.getResponseHeaders());
            if (this.skipCount >= 0) {
                // Add Offset Continuation Token
                String sourceContinuationToken = tFeedResponse.getResponseContinuation();
                OffsetContinuationToken offsetContinuationToken = new OffsetContinuationToken(this.skipCount,
                        sourceContinuationToken);
                headers.put(HttpConstants.HttpHeaders.CONTINUATION, offsetContinuationToken.toJson());
            }

            return BridgeInternal.createFeedResponseWithQueryMetrics(documentsAfterSkip, headers,
                    tFeedResponse.getQueryMetrics());
        });
    }

    IDocumentQueryExecutionComponent<T> getComponent() {
        return this.component;
    }

}
