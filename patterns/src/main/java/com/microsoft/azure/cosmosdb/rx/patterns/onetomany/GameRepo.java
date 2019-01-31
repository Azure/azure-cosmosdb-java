package com.microsoft.azure.cosmosdb.rx.patterns.onetomany;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.microsoft.azure.cosmosdb.*;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import com.microsoft.azure.cosmosdb.rx.patterns.UriFactory;
import rx.Observable;

import java.util.Date;
import java.util.List;

public class GameRepo {

    private AsyncDocumentClient client;
    private final String databaseName;
    private final String collectionName = "games";
    private static Gson gson;

    public GameRepo(AsyncDocumentClient client, String databaseName) {
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

        // TIP: If queries are known upfront, index just the properties you need
        var partitionKeyDefinition = new PartitionKeyDefinition();
        var paths = List.of("/playerId");
        partitionKeyDefinition.setPaths(paths);
        collection.setPartitionKey(partitionKeyDefinition);

        var indexingPolicy = new IndexingPolicy();
        indexingPolicy.setAutomatic(true);
        indexingPolicy.setIndexingMode(IndexingMode.Consistent);
        indexingPolicy.getIncludedPaths().clear();
        indexingPolicy.getExcludedPaths().clear();

        var incPath = new IncludedPath();
        incPath.setPath("/playerId/?");
        incPath.setIndexes(List.of(new RangeIndex(DataType.String,-1)));

        var includedPaths = List.of(incPath);
        indexingPolicy.setIncludedPaths(includedPaths);

        var excludedPath = new ExcludedPath();
        excludedPath.setPath("/*");
        var excludedPaths = List.of(excludedPath);
        indexingPolicy.setExcludedPaths(excludedPaths);

        collection.setIndexingPolicy(indexingPolicy);

        var requestOptions = new RequestOptions();
        requestOptions.setOfferThroughput(10000);

        return client.createCollection(UriFactory.createDatabaseUri(databaseName),collection,requestOptions);

    }

    public Observable<Game> AddGameAsync(Game game) {
        var collectionUri = UriFactory.createCollectionUri(databaseName,collectionName);

        return client.upsertDocument(collectionUri,game,null,true)
                .map((resp) -> gson.fromJson(resp.getResource().toString(),Game.class));
    }

    public Observable<Game> GetGameAsync(String playerId, String gameId) {
        // TIP: When partition key != id, ensure it is passed in via GET (not cross-partition query on gameId)
        var documentUri = UriFactory.createDocumentUri(databaseName,collectionName,gameId);
        var requestOptions = new RequestOptions();
        requestOptions.setPartitionKey(new PartitionKey(playerId));

        return client.readDocument(documentUri,requestOptions)
                .map((resp) -> gson.fromJson(resp.getResource().toString(),Game.class));
    }

    public Observable<Game> GetGamesAsync(String playerId) {
        // TIP: Favor single-partition queries (with pk in filter)
        var collectionUri = UriFactory.createCollectionUri(databaseName,collectionName);

        return client.queryDocuments(collectionUri,
                        "SELECT * FROM root r WHERE r.playerId='" + playerId + "'", null)
                .flatMapIterable(FeedResponse::getResults)
                .map((doc) -> gson.fromJson(doc.toString(),Game.class));
    }

    public Observable<Game> UpdateGameAsync(Game gameInfo) {
        return GetGameAsync(gameInfo.getPlayerId(), gameInfo.getId())
                .flatMap(player -> {

                    var documentUri = UriFactory.createDocumentUri(databaseName,collectionName,gameInfo.getId());

                    var accessCondition = new AccessCondition();
                    accessCondition.setCondition(player.get_etag());
                    accessCondition.setType(AccessConditionType.IfMatch);

                    var requestOptions = new RequestOptions();
                    requestOptions.setAccessCondition(accessCondition);

                    return client.replaceDocument(documentUri, gameInfo, requestOptions)
                            .map((resp) -> gson.fromJson(resp.getResource().toString(),Game.class));

                });
    }

    public Observable<ResourceResponse<Document>> RemoveGameAsync(String playerId, String gameId) {
        var documentUri = UriFactory.createDocumentUri(databaseName,collectionName,gameId);
        var requestOptions = new RequestOptions();
        requestOptions.setPartitionKey(new PartitionKey(playerId));

        return client.deleteDocument(documentUri,requestOptions);
    }

}
