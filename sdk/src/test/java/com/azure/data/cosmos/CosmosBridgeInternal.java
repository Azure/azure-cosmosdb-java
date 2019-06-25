package com.azure.data.cosmos;

import reactor.core.publisher.Mono;

public class CosmosBridgeInternal {
    
    public static DocumentCollection toDocumentCollection(CosmosContainerSettings cosmosContainerSettings) {
        return new DocumentCollection(cosmosContainerSettings.toJson());
    }

    public static AsyncDocumentClient getAsyncDocumentClient(CosmosClient client) {
        return client.getDocClientWrapper();
    }
    
    public static CosmosDatabase getCosmosDatabaseWithNewClient(CosmosDatabase cosmosDatabase, CosmosClient client) {
        return new CosmosDatabase(cosmosDatabase.id(), client);
    }
    
    public static CosmosContainer getCosmosContainerWithNewClient(CosmosContainer cosmosContainer, CosmosDatabase cosmosDatabase, CosmosClient client) {
        return new CosmosContainer(cosmosContainer.id(), CosmosBridgeInternal.getCosmosDatabaseWithNewClient(cosmosDatabase, client));
    }

    public static Mono<DatabaseAccount> getDatabaseAccount(CosmosClient client) {
        return client.getDatabaseAccount();
    }
}
