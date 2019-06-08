package com.microsoft.azure.cosmos;

import com.microsoft.azure.cosmosdb.RequestOptions;
import com.microsoft.azure.cosmosdb.internal.Paths;
import reactor.core.publisher.Mono;

public class CosmosUser extends CosmosResource {
    CosmosDatabase database;
    public CosmosUser(String id, CosmosDatabase database) {
        super(id);
        this.database = database;
    }

    /**
     * Reads a cosmos user
     *
     * @return a {@link Mono} containing the single resource response with the read user or an error.
     */
    public Mono<CosmosUserResponse> read() {
        return this.read(null);
    }

    /**
     * Reads a cosmos user
     *
     * @param options the request options
     * @return a {@link Mono} containing the single resource response with the read user or an error.
     */
    public Mono<CosmosUserResponse> read(RequestOptions options) {
        return this.database.getDocClientWrapper()
                .readUser(getLink(), options)
                .map(response -> new CosmosUserResponse(response, database)).single();
    }

    /**
     * Replace a cosmos user
     *
     * @param userSettings the user settings to use
     * @param options      the request options
     * @return a {@link Mono} containing the single resource response with the replaced user or an error.
     */
    public Mono<CosmosUserResponse> replace(CosmosUserSettings userSettings, RequestOptions options) {
        return this.database.getDocClientWrapper()
                .replaceUser(userSettings.getV2User(), options)
                .map(response -> new CosmosUserResponse(response, database)).single();
    }

    /**
     * Delete a cosmos user
     *
     * @param options the request options
     * @return a {@link Mono} containing the single resource response with the deleted user or an error.
     */
    public Mono<CosmosUserResponse> delete(RequestOptions options) {
        return this.database.getDocClientWrapper()
                .deleteUser(getLink(), options)
                .map(response -> new CosmosUserResponse(response, database)).single();
    }

    @Override
    protected String getURIPathSegment() {
        return Paths.USERS_PATH_SEGMENT;
    }

    @Override
    protected String getParentLink() {
        return database.getLink()   ;
    }
}
