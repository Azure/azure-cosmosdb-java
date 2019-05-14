package com.microsoft.azure.cosmos;

import com.microsoft.azure.cosmosdb.Document;
import com.microsoft.azure.cosmosdb.internal.Paths;
import hu.akarnokd.rxjava.interop.RxJavaInterop;
import reactor.adapter.rxjava.RxJava2Adapter;
import reactor.core.publisher.Mono;

public class CosmosItem extends CosmosResource{
    private Object partitionKey;
    private CosmosContainer container;

     CosmosItem(String id, Object partitionKey, CosmosContainer container) {
        super(id);
        this.partitionKey = partitionKey;
        this.container = container;
    }

    /**
     * Reads an item.
     *
     * After subscription the operation will be performed. 
     * The {@link Mono} upon successful completion will contain a cosmos item response with the read item
     * In case of failure the {@link Mono} will error.
     *
     * @return an {@link Mono} containing the cosmos item response with the read item or an error
     */
    public Mono<CosmosItemResponse> read() {
        return read(new CosmosItemRequestOptions(partitionKey));
    }

    /**
     * Reads an item.
     *
     * After subscription the operation will be performed. 
     * The {@link Mono} upon successful completion will contain a cosmos item response with the read item
     * In case of failure the {@link Mono} will error.
     *
     * @param requestOptions the request comosItemRequestOptions
     * @return an {@link Mono} containing the cosmos item response with the read item or an error
     */
    public Mono<CosmosItemResponse> read(CosmosItemRequestOptions requestOptions) {
        return RxJava2Adapter.singleToMono(RxJavaInterop.toV2Single(container.getDatabase().getDocClientWrapper()
                .readDocument(getLink(), requestOptions.toRequestOptions())
                .map(response -> new CosmosItemResponse(response, requestOptions.getPartitionKey(), container)).toSingle()));
    }

    /**
     * Replaces an item with the passed in item.
     *
     * After subscription the operation will be performed. 
     * The {@link Mono} upon successful completion will contain a single cosmos item response with the replaced item.
     * In case of failure the {@link Mono} will error.
     *
     * @param item the item to replace (containing the document id).
     * @return an {@link Mono} containing the  cosmos item resource response with the replaced item or an error.
     */
    public Mono<CosmosItemResponse> replace(Object item){
        return replace(item, new CosmosItemRequestOptions(partitionKey));
    }

    /**
     * Replaces an item with the passed in item.
     *
     * After subscription the operation will be performed. 
     * The {@link Mono} upon successful completion will contain a single cosmos item response with the replaced item.
     * In case of failure the {@link Mono} will error.
     *
     * @param item the item to replace (containing the document id).
     * @param requestOptions the request comosItemRequestOptions
     * @return an {@link Mono} containing the  cosmos item resource response with the replaced item or an error.
     */
    public Mono<CosmosItemResponse> replace(Object item, CosmosItemRequestOptions requestOptions){
        Document doc = CosmosItemSettings.fromObject(item);
        return RxJava2Adapter.singleToMono(RxJavaInterop.toV2Single(container.getDatabase()
                .getDocClientWrapper()
                .replaceDocument(getLink(), doc, requestOptions.toRequestOptions())
                .map(response -> new CosmosItemResponse(response, requestOptions.getPartitionKey(), container)).toSingle()));
    }

    /**
     * Deletes the item.
     *
     * After subscription the operation will be performed. 
     * The {@link Mono} upon successful completion will contain a single cosmos item response with the replaced item.
     * In case of failure the {@link Mono} will error.
     * @return an {@link Mono} containing the  cosmos item resource response.
     */
    public Mono<CosmosItemResponse> delete() {
        return delete(new CosmosItemRequestOptions(partitionKey));
    }

    /**
     * Deletes the item.
     *
     * After subscription the operation will be performed. 
     * The {@link Mono} upon successful completion will contain a single cosmos item response with the replaced item.
     * In case of failure the {@link Mono} will error.
     *
     * @param options the request options
     * @return an {@link Mono} containing the  cosmos item resource response.
     */
    public Mono<CosmosItemResponse> delete(CosmosItemRequestOptions options){
        return RxJava2Adapter.singleToMono(
                RxJavaInterop.toV2Single(container.getDatabase()
                        .getDocClientWrapper()
                        .deleteDocument(getLink(),options.toRequestOptions())
                        .map(response -> new CosmosItemResponse(response, options.getPartitionKey(), container))
                        .toSingle()));
    }
    
    void setContainer(CosmosContainer container) {
        this.container = container;
    }

    @Override
    protected String getURIPathSegment() {
        return Paths.DOCUMENTS_PATH_SEGMENT;
    }

    @Override
    protected String getParentLink() {
        return this.container.getLink();
    }

}
