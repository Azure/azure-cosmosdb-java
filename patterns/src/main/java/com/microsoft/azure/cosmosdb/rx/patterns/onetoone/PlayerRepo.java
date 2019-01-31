package com.microsoft.azure.cosmosdb.rx.patterns.onetoone;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.microsoft.azure.cosmosdb.*;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import com.microsoft.azure.cosmosdb.rx.patterns.UriFactory;
import rx.Observable;

import java.util.Date;
import java.util.List;


public class PlayerRepo {

    private AsyncDocumentClient client;
    private final String databaseName;
    private final String collectionName = "players";
    private static Gson gson;

    public PlayerRepo(AsyncDocumentClient client, String databaseName) {
        this.client = client;
        this.databaseName = databaseName;

        // We'll use Gson for POJO <=> JSON serialization.
        // set the appropriate DateFormatter
        JsonDeserializer<Date> dateJsonDeserializer =
                (json, typeOfT, context) -> json == null ? null : new Date(json.getAsLong());
        gson = new GsonBuilder().registerTypeAdapter(Date.class,dateJsonDeserializer).create();
    }

    public Observable<ResourceResponse<DocumentCollection>> CreateCollectionAsync() {
        var collection = new DocumentCollection();
        collection.setId(collectionName);

        // TIP: ID may be a good choice for partition key
        var partitionKeyDefinition = new PartitionKeyDefinition();
        var paths = List.of("/id");
        partitionKeyDefinition.setPaths(paths);
        collection.setPartitionKey(partitionKeyDefinition);

        // TIP: Disable indexing if it's just KV lookup
        var indexingPolicy = new IndexingPolicy();
        indexingPolicy.setAutomatic(false);
        indexingPolicy.setIndexingMode(IndexingMode.None);
        indexingPolicy.getIncludedPaths().clear();
        indexingPolicy.getExcludedPaths().clear();
        collection.setIndexingPolicy(indexingPolicy);

        var requestOptions = new RequestOptions();
        requestOptions.setOfferThroughput(10000);

        return client.createCollection(UriFactory.createDatabaseUri(databaseName),collection,requestOptions);

    }

    public Observable<Player> AddPlayerAsync(Player player) {
        var collectionUri = UriFactory.createCollectionUri(databaseName,collectionName);

        // TIP: Use the atomic Upsert for Insert or Replace
        return client.upsertDocument(collectionUri,player,null,true)
                .map((resp) -> gson.fromJson(resp.getResource().toString(),Player.class));
    }

    public Observable<Player> GetPlayerAsync(String playerId) {
        var documentUri = UriFactory.createDocumentUri(databaseName,collectionName,playerId);
        var requestOptions = new RequestOptions();
        requestOptions.setPartitionKey(new PartitionKey(playerId));

        // TIP: Use GET over query when possible
        return client.readDocument(documentUri,requestOptions)
                .map((resp) -> gson.fromJson(resp.getResource().toString(),Player.class));
    }

    public Observable<Player> UpdatePlayerAsync(Player playerInfo) {
        return GetPlayerAsync(playerInfo.getId())
                .flatMap(player -> {
                    // TIP: Use conditional update with ETag
                    var documentUri = UriFactory.createDocumentUri(databaseName,collectionName,playerInfo.getId());

                    var accessCondition = new AccessCondition();
                    accessCondition.setCondition(player.get_etag());
                    accessCondition.setType(AccessConditionType.IfMatch);

                    var requestOptions = new RequestOptions();
                    requestOptions.setAccessCondition(accessCondition);

                    return client.replaceDocument(documentUri, playerInfo, requestOptions)
                            .map((resp) -> gson.fromJson(resp.getResource().toString(),Player.class));

                });
    }

    public Observable<ResourceResponse<Document>> RemovePlayerAsync(String playerId) {
        var documentUri = UriFactory.createDocumentUri(databaseName,collectionName,playerId);
        var requestOptions = new RequestOptions();
        requestOptions.setPartitionKey(new PartitionKey(playerId));

        return client.deleteDocument(documentUri,requestOptions);
    }

}
