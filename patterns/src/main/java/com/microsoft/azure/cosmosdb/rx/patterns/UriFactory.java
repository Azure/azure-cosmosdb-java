package com.microsoft.azure.cosmosdb.rx.patterns;

public class UriFactory {

    public static String createDatabaseUri (String databaseId) {
        return "dbs/" + databaseId;
    }

    public static String createCollectionUri(String databaseId, String collectionId) {
        return "dbs/" + databaseId+ "/colls/" + collectionId;
    }


    public static String createDocumentUri(String databaseId, String collectionId, String documentId) {
        return "dbs/" + databaseId+ "/colls/" + collectionId + "/docs/" + documentId;
    }
}
