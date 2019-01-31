package com.microsoft.azure.cosmosdb.rx.patterns.TimeSeries;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.microsoft.azure.cosmosdb.*;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import com.microsoft.azure.cosmosdb.rx.patterns.UriFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;
import rx.schedulers.Schedulers;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class SensorReadingRepo {
    private AsyncDocumentClient client;
    private final String databaseName;
    private final String rawReadingsCollectionName = "rawReadings";
    private final String hourlyReadingsCollectionName = "hourlyRollups";
    private static Gson gson;
    private static final Logger LOGGER = LoggerFactory.getLogger(SensorReadingRepo.class);


    public SensorReadingRepo(AsyncDocumentClient client, String databaseName) {
        this.client = client;
        this.databaseName = databaseName;

        // We'll use Gson for POJO <=> JSON serialization.
        gson = new GsonBuilder().create();
    }

    public Observable<ResourceResponse<DocumentCollection>> CreateCollectionsByHour() {
        //TIP: pre-aggregate rollups vs. querying raw data when possible
        return Observable.merge(CreateSensorCollectionAsync(rawReadingsCollectionName, 500, 7),
                CreateSensorCollectionAsync(hourlyReadingsCollectionName, 400, 90));
    }

    private Observable<ResourceResponse<DocumentCollection>>
    CreateSensorCollectionAsync(String collectionName, int throughput, int expirationDays){
        var collection = new DocumentCollection();
        collection.setId(collectionName);

        //TIP: use a fine-grained PK like sensorId, not timestamp
        var partitionKeyDefinition = new PartitionKeyDefinition();
        var paths = List.of("/sensorId");
        partitionKeyDefinition.setPaths(paths);
        collection.setPartitionKey(partitionKeyDefinition);

        var indexingPolicy = new IndexingPolicy();
        indexingPolicy.setAutomatic(true);
        indexingPolicy.setIndexingMode(IndexingMode.Consistent);
        indexingPolicy.getIncludedPaths().clear();
        indexingPolicy.getExcludedPaths().clear();

        var sensorIdPath = new IncludedPath();
        sensorIdPath.setPath("/sensorId/?");
        sensorIdPath.setIndexes(List.of(new RangeIndex(DataType.String,-1)));

        var timeStampPath = new IncludedPath();
        timeStampPath.setPath("/unixTimeStamp/?");
        timeStampPath.setIndexes(List.of(new RangeIndex(DataType.Number,-1)));

        var includedPaths = List.of(sensorIdPath, timeStampPath);
        indexingPolicy.setIncludedPaths(includedPaths);

        var excludedPath = new ExcludedPath();
        excludedPath.setPath("/*");
        var excludedPaths = List.of(excludedPath);
        indexingPolicy.setExcludedPaths(excludedPaths);

        collection.setIndexingPolicy(indexingPolicy);

        //TIP: set TTL for data expiration
        collection.setDefaultTimeToLive(expirationDays * 86400);

        var requestOptions = new RequestOptions();
        requestOptions.setOfferThroughput(throughput);

        return client.createCollection(UriFactory.createDatabaseUri(databaseName),collection,requestOptions);
    }

    public Observable<SensorReading> AddSensorReadingAsync(SensorReading reading) {
        var collectionUri = UriFactory.createCollectionUri(databaseName,rawReadingsCollectionName);
        reading.setId(Instant.ofEpochSecond(reading.getUnixTimeStamp()).toString());
        return client.upsertDocument(collectionUri,reading,null,true)
                .map((resp) -> gson.fromJson(resp.getResource().toString(),SensorReading.class));
    }

    public Observable<SensorReading> GetSensorReadingsForTimeRangeAsync(String sensorId, Date startTime, Date endTime){
        var startTimeMillis = startTime.toInstant().toEpochMilli();
        var endTimeMillis = endTime.toInstant().toEpochMilli();
        var collectionUri = UriFactory.createCollectionUri(databaseName,rawReadingsCollectionName);

        return client.queryDocuments(collectionUri,
                "SELECT * FROM root r WHERE r.sensorId='" + sensorId + "'"
                +" and r.unixTimeStamp >= " + startTimeMillis + " and r.unixTimeStamp < " + endTimeMillis, null)
                .flatMapIterable(FeedResponse::getResults)
                .map((doc) -> gson.fromJson(doc.toString(),SensorReading.class));
    }

    public Observable<SensorReadingRollup> GetHourlySensorReadings(String sensorId){

        var collectionUri = UriFactory.createCollectionUri(databaseName,hourlyReadingsCollectionName);

        return client.queryDocuments(collectionUri,
                "SELECT * FROM root r WHERE r.sensorId='" + sensorId + "'",null)
                .flatMapIterable(FeedResponse::getResults)
                .map((doc) -> gson.fromJson(doc.toString(),SensorReadingRollup.class));
    }

    public Subscription SyncHourlyRollupCollectionAsync() {
        //TIP: Use change feed to build time-based rollups
        String sourceCollectionUri = UriFactory.createCollectionUri(databaseName, rawReadingsCollectionName);
        Map<String,String> checkPoints = new HashMap<>();

        return Observable.interval(0,1, TimeUnit.SECONDS)
                .flatMap((x) -> client.readPartitionKeyRanges(sourceCollectionUri,null))
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

                    return client.queryDocumentChangeFeed(sourceCollectionUri,feedOptions)
                            .doOnError(e -> LOGGER.error("Error : ",e ))
                            .flatMapIterable(resp -> {
                                checkPoints.put(pkRangeId,resp.getResponseContinuation());
                                return resp.getResults();
                            })
                            .map(changedDoc -> gson.fromJson(changedDoc.toString(),SensorReading.class))
                            .concatMap(reading -> updateHourlyRollupCollectionAsync(reading));
                })
                .subscribeOn(Schedulers.io())
                .doOnUnsubscribe(()-> LOGGER.info("Sync Hourly Rollup Collection unsubscribed"))
                .publish()
                .connect();
    }

    private String getHourlyIdFromUnixTimeStamp(Long timeStamp)
    {
        Date date = Date.from(Instant.ofEpochSecond(timeStamp));
        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH'Z'"); // Quoted "Z" to indicate UTC, no timezone offset
        df.setTimeZone(tz);
        return df.format(date);
    }

    private Observable<ResourceResponse<Document>> updateHourlyRollupCollectionAsync(SensorReading reading){

        String destCollectionUri = UriFactory.createCollectionUri(databaseName, hourlyReadingsCollectionName);
        String hourlyRollupId = getHourlyIdFromUnixTimeStamp(reading.getUnixTimeStamp());
        String sensorRollupUri = UriFactory.createDocumentUri(databaseName,hourlyReadingsCollectionName,hourlyRollupId);

        var requestOptions = new RequestOptions();
        requestOptions.setPartitionKey(new PartitionKey(reading.getSensorId()));

        return client.readDocument(sensorRollupUri,requestOptions)
                .onErrorReturn(ex -> null)
                .map((resp) -> {
                    if(resp == null)
                        return SensorReadingRollup.builder().id(hourlyRollupId)
                                .sensorId(reading.getSensorId()).siteId(reading.getSiteId())
                                .count(0).sumPressure(0.0).sumTemperature(0.0).build();
                    return gson.fromJson(resp.getResource().toString(),SensorReadingRollup.class);
                })
                .flatMap(rollup -> {
                    rollup.setCount(rollup.getCount() + 1);
                    rollup.setSumTemperature(rollup.getSumTemperature() + reading.getTemperature());
                    rollup.setSumPressure(rollup.getSumPressure() + reading.getPressure());
                    return client.upsertDocument(destCollectionUri,rollup,null,true)
                            .doOnNext(r -> LOGGER.info("Updated changes for document "+ r.getResource().getId()))
                            .doOnError(e -> LOGGER.error("Error : ",e ));
                });

    }

}
