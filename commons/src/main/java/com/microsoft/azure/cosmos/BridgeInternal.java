package com.microsoft.azure.cosmos;

import com.microsoft.azure.cosmosdb.DocumentCollection;

public class BridgeInternal {

    public static String getLink(CosmosResource resource) {
        return resource.getLink();
    }
    
    public static DocumentCollection toDocumentCollection(CosmosContainerSettings cosmosContainerSettings) {
        return new DocumentCollection(cosmosContainerSettings.toJson());
    }
}
