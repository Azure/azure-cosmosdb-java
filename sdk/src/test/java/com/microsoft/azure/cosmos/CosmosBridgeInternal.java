package com.microsoft.azure.cosmos;

import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;

public class CosmosBridgeInternal {

    public static AsyncDocumentClient getAsyncDocumentClient(CosmosClient client) {
        return client.getDocClientWrapper();
    }
}
