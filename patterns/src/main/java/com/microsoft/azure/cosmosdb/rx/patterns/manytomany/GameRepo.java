package com.microsoft.azure.cosmosdb.rx.patterns.manytomany;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.microsoft.azure.cosmosdb.*;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import com.microsoft.azure.cosmosdb.rx.patterns.UriFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;
import rx.schedulers.Schedulers;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class GameRepo {

    private AsyncDocumentClient client;
    private final String databaseName;
    private final String playerLookupCollectionName = "gamesByPlayerId";
    private final String gameLookupCollectionName = "gamesByGameId";
    private static Gson gson;
    private static final Logger LOGGER = LoggerFactory.getLogger(Game.class);


    public GameRepo(AsyncDocumentClient client, String databaseName) {
        this.client = client;
        this.databaseName = databaseName;

        // We'll use Gson for POJO <=> JSON serialization.
        // set the appropriate DateFormatter
        JsonDeserializer<Date> dateJsonDeserializer =
                (json, typeOfT, context) -> json == null ? null : new Date(json.getAsLong());
        gson = new GsonBuilder().registerTypeAdapter(Date.class,dateJsonDeserializer).create();
    }

    public Observable<ResourceResponse<DocumentCollection>> CreatePlayerLookupCollectionAsync() {
        var collection = new DocumentCollection();
        collection.setId(playerLookupCollectionName);

        var partitionKeyDefinition = new PartitionKeyDefinition();
        var paths = List.of("/playerId");
        partitionKeyDefinition.setPaths(paths);
        collection.setPartitionKey(partitionKeyDefinition);

        var indexingPolicy = new IndexingPolicy();
        indexingPolicy.setAutomatic(true);
        indexingPolicy.setIndexingMode(IndexingMode.Consistent);
        indexingPolicy.getIncludedPaths().clear();
        indexingPolicy.getExcludedPaths().clear();

        var playerPath = new IncludedPath();
        playerPath.setPath("/playerId/?");
        playerPath.setIndexes(List.of(new RangeIndex(DataType.String,-1)));

        var gamePath = new IncludedPath();
        gamePath.setPath("/gameId/?");
        gamePath.setIndexes(List.of(new RangeIndex(DataType.String,-1)));

        var includedPaths = List.of(playerPath, gamePath);
        indexingPolicy.setIncludedPaths(includedPaths);

        var excludedPath = new ExcludedPath();
        excludedPath.setPath("/*");
        var excludedPaths = List.of(excludedPath);
        indexingPolicy.setExcludedPaths(excludedPaths);

        collection.setIndexingPolicy(indexingPolicy);

        var requestOptions = new RequestOptions();
        requestOptions.setOfferThroughput(1000);

        return client.createCollection(UriFactory.createDatabaseUri(databaseName),collection,requestOptions);

    }

    public Observable<Game> AddGameAsync(Game game) {
        var collectionUri = UriFactory.createCollectionUri(databaseName,playerLookupCollectionName);

        return client.upsertDocument(collectionUri,game,null,false)
                .map((resp) -> gson.fromJson(resp.getResource().toString(),Game.class));
    }

    public Observable<Game> GetGameByPlayerIdAsync(String playerId) {
        var collectionUri = UriFactory.createCollectionUri(databaseName,playerLookupCollectionName);

        return client.queryDocuments(collectionUri,
                        "SELECT * FROM root r WHERE r.playerId='" + playerId + "'", null)
                .flatMapIterable(FeedResponse::getResults)
                .map((doc) -> gson.fromJson(doc.toString(),Game.class));
    }


    public Observable<Game> GetGamesInfrequentAsync(String gameId) {
        //TIP: use cross-partition query for relatively infrequent queries. They're served from the index too
        var collectionUri = UriFactory.createCollectionUri(databaseName,playerLookupCollectionName);

        var feedOptions = new FeedOptions();
        feedOptions.setEnableCrossPartitionQuery(true);
        feedOptions.setMaxDegreeOfParallelism(-1);

        return client.queryDocuments(collectionUri,
                "SELECT * FROM root r WHERE r.gameId ='" + gameId + "'", feedOptions)
                .flatMapIterable(FeedResponse::getResults)
                .map((doc) -> gson.fromJson(doc.toString(),Game.class));
    }

    public Observable<ResourceResponse<DocumentCollection>> CreateGameLookupCollectionAsync() {
        //TIP: Use a second collection pivoted by a different partition key if the mix is 50:50
        var collection = new DocumentCollection();
        collection.setId(gameLookupCollectionName);

        var partitionKeyDefinition = new PartitionKeyDefinition();
        var paths = List.of("/gameId");
        partitionKeyDefinition.setPaths(paths);
        collection.setPartitionKey(partitionKeyDefinition);

        var indexingPolicy = new IndexingPolicy();
        indexingPolicy.setAutomatic(true);
        indexingPolicy.setIndexingMode(IndexingMode.Consistent);
        indexingPolicy.getIncludedPaths().clear();
        indexingPolicy.getExcludedPaths().clear();

        var incPath = new IncludedPath();
        incPath.setPath("/gameId/?");
        incPath.setIndexes(List.of(new RangeIndex(DataType.String,-1)));

        var includedPaths = List.of(incPath);
        indexingPolicy.setIncludedPaths(includedPaths);

        var excludedPath = new ExcludedPath();
        excludedPath.setPath("/*");
        var excludedPaths = List.of(excludedPath);
        indexingPolicy.setExcludedPaths(excludedPaths);

        collection.setIndexingPolicy(indexingPolicy);

        var requestOptions = new RequestOptions();
        requestOptions.setOfferThroughput(1000);

        return client.createCollection(UriFactory.createDatabaseUri(databaseName),collection,requestOptions);

    }

    public Observable<Game> GetGamesByGameIdAsync(String gameId) {
        var collectionUri = UriFactory.createCollectionUri(databaseName,gameLookupCollectionName);

        return client.queryDocuments(collectionUri,
                "SELECT * FROM root r WHERE r.gameId ='" + gameId + "'", null)
                .flatMapIterable(FeedResponse::getResults)
                .map((doc) -> gson.fromJson(doc.toString(),Game.class));
    }

    public Subscription SyncGameLookupCollectionAsync() {
        //TIP: Prefer change feed over double-writes
        String sourceCollectionUri = UriFactory.createCollectionUri(databaseName, playerLookupCollectionName);
        String destCollectionUri = UriFactory.createCollectionUri(databaseName, gameLookupCollectionName);

        //TIP: Use change feed processor library for built-in checkpointing/load balancing/failure recovery
        Map<String,String> checkPoints = new HashMap<>();

        return Observable.interval(0,1, TimeUnit.SECONDS)
                .flatMap(x -> client.readPartitionKeyRanges(sourceCollectionUri,null))
                .doOnError(e -> LOGGER.error("Error : ",e ))
                .flatMapIterable(FeedResponse::getResults)
                .map(PartitionKeyRange::getId)
                .flatMap(pkRangeId -> {
                    var feedOptions = new ChangeFeedOptions();
                    feedOptions.setPartitionKeyRangeId(pkRangeId);

                    var continuation = checkPoints.get(pkRangeId);
                    if(continuation == null)
                        feedOptions.setStartFromBeginning(true);
                    else
                        feedOptions.setRequestContinuation(continuation);

                    //TIP: Azure Functions, Spark Streaming also internally use change feed and provide primitives
                    return client.queryDocumentChangeFeed(sourceCollectionUri,feedOptions)
                            .doOnError(e -> LOGGER.error("Error : ",e ))
                            .flatMapIterable(resp -> {
                                checkPoints.put(pkRangeId,resp.getResponseContinuation());
                                return resp.getResults();

                        })
                            .flatMap(changedDoc -> client.upsertDocument(destCollectionUri,changedDoc,null,true))
                            .doOnNext(r -> LOGGER.info("Updated changes for document "+ r.getResource().getId()))
                            .doOnError(e -> LOGGER.error("Error : ",e ));
                })
                .subscribeOn(Schedulers.io())
                .doOnUnsubscribe(()-> LOGGER.info("Sync Game Lookup Collection unsubscribed"))
                .publish()
                .connect();
    }
}
