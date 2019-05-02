package com.microsoft.azure.cosmosdb.internal.directconnectivity.rntbd;

import com.microsoft.azure.cosmosdb.BridgeInternal;
import com.microsoft.azure.cosmosdb.DocumentClientException;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.RequestTimeoutException;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.StoreResponse;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkArgument;

class RntbdRequestRecord {

    private final RntbdRequestArgs args;
    private final CompletableFuture<? super StoreResponse> future;

    RntbdRequestRecord(final RntbdRequestArgs args, final CompletableFuture<? super StoreResponse> future) {
        this.args = args;
        this.future = future;
    }

    RntbdRequestArgs getArgs() {
        return this.args;
    }

    long getBirthTime() {
        return this.args.getBirthTime();
    }

    Duration getLifetime() {
        return this.args.getLifetime();
    }

    boolean isDone() {
        return this.future.isDone();
    }

    void cancel() {
        this.future.cancel(true);
    }

    void complete(final StoreResponse response) {
        this.future.complete(response);
    }

    void completeExceptionally(final Throwable throwable) {
        checkArgument(throwable instanceof DocumentClientException, "throwable");
        this.future.completeExceptionally(throwable);
    }

    void expire() {
        final RequestTimeoutException error = new RequestTimeoutException(
            "Request timeout interval elapsed", this.args.getPhysicalAddress());
        BridgeInternal.setRequestHeaders(error, this.args.getServiceRequest().getHeaders());
        this.completeExceptionally(error);
    }

    @Override
    public String toString() {
        return this.args.toString();
    }
}
