package com.microsoft.azure.cosmosdb.rx.patterns.TimeSeries;

import com.microsoft.azure.cosmosdb.ConnectionPolicy;
import com.microsoft.azure.cosmosdb.ConsistencyLevel;
import com.microsoft.azure.cosmosdb.Database;
import com.microsoft.azure.cosmosdb.DocumentClientException;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import com.microsoft.azure.cosmosdb.rx.patterns.TestConfigurations;
import com.microsoft.azure.cosmosdb.rx.patterns.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;
import rx.observers.TestSubscriber;
import rx.schedulers.Schedulers;

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.microsoft.azure.cosmosdb.rx.patterns.TestUtils.HTTP_STATUS_CODE_CREATED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class SensorReadingRepoTest {
    private static final String DATABASE_NAME = TestUtils.getDatabaseName(SensorReadingRepoTest.class);
    private AsyncDocumentClient client;

    @Before
    public void setUp() throws DocumentClientException {
        client = new AsyncDocumentClient.Builder()
                .withServiceEndpoint(TestConfigurations.HOST)
                .withMasterKeyOrResourceToken(TestConfigurations.MASTER_KEY)
                .withConnectionPolicy(ConnectionPolicy.GetDefault())
                .withConsistencyLevel(ConsistencyLevel.Session)
                .build();

        // Clean up before setting up
        TestUtils.cleanUpDatabase(client,DATABASE_NAME);

        var db = new Database();
        db.setId(DATABASE_NAME);
        client.createDatabase(db, null)
                .toBlocking()
                .subscribe();
    }

    @After
    public void shutdown() {
        TestUtils.safeclean(client, DATABASE_NAME);
    }

    @Test
    public void testCreateCollectionsByHour() {
        var sensorReadingRepo = new SensorReadingRepo(client,DATABASE_NAME);

        var actualList = sensorReadingRepo.CreateCollectionsByHour()
                .toList()
                .toBlocking()
                .single();

        for (var resourceResp : actualList) {
            assertThat(resourceResp.getStatusCode(),equalTo(HTTP_STATUS_CODE_CREATED));
        }
    }

    @Test
    public void testAddSensorReadingAsync() {
        var sensorReadingRepo = new SensorReadingRepo(client,DATABASE_NAME);
        sensorReadingRepo.CreateCollectionsByHour()
                .toBlocking()
                .subscribe();

        var sensorReading  = createSensor("1");

        var subscriber = new TestSubscriber<SensorReading>();
        sensorReadingRepo.AddSensorReadingAsync(sensorReading)
                .toBlocking()
                .subscribe(subscriber);

        subscriber.assertCompleted();
        subscriber.assertNoErrors();
        subscriber.assertValueCount(1);

    }

    @Test
    public void testGetSensorReadingsForTimeRangeAsync() {
        var sensorReadingRepo = new SensorReadingRepo(client,DATABASE_NAME);
        sensorReadingRepo.CreateCollectionsByHour()
                .toBlocking()
                .subscribe();

        var numberOfReadings = 10;
        Date startTime = new Date();
        addSensorReadings("1",numberOfReadings);
        Date endTime = new Date();
        // Adding 10 more readings which should not show up in results
        addSensorReadings("1",numberOfReadings);

        var subscriber = new TestSubscriber<SensorReading>();
        sensorReadingRepo.GetSensorReadingsForTimeRangeAsync("1",startTime,endTime)
                .toBlocking()
                .subscribe(subscriber);

        subscriber.assertCompleted();
        subscriber.assertNoErrors();
        subscriber.assertValueCount(numberOfReadings);

    }

    @Test
    public void testSyncUpAndGetGameAsync() throws Exception {
        var sensorReadingRepo = new SensorReadingRepo(client,DATABASE_NAME);

        sensorReadingRepo.CreateCollectionsByHour()
                .toBlocking()
                .subscribe();

        var currHourReading1 = SensorReading.builder().sensorId("1").siteId("site1").pressure(1.0)
                .temperature(1.0).unixTimeStamp(1548740162000L).build();

        var currHourReading2 = SensorReading.builder().sensorId("1").siteId("site1").pressure(2.0)
                .temperature(2.0).unixTimeStamp(1548740162002L).build();

        var nextHourReading1 = SensorReading.builder().sensorId("1").siteId("site1").pressure(10.0)
                .temperature(10.0).unixTimeStamp(1548743762000L).build();

        var nextHourReading2 = SensorReading.builder().sensorId("1").siteId("site1").pressure(20.0)
                .temperature(20.0).unixTimeStamp(1548743762002L).build();

        Observable
                .just(currHourReading1,currHourReading2,nextHourReading1,nextHourReading2)
                .flatMap(sensorReading ->
                        sensorReadingRepo
                                .AddSensorReadingAsync(sensorReading)
                                .subscribeOn(Schedulers.io()))
                .toBlocking()
                .subscribe();

        var expectedTemperatureSumList = List.of(3.0,30.0);

        var syncSubscription = sensorReadingRepo.SyncHourlyRollupCollectionAsync();

        // Sleep to give enough time for syncup
        Thread.sleep(5000);

        var actualList =  sensorReadingRepo.GetHourlySensorReadings("1")
                .toList()
                .toBlocking()
                .single();

        var actualTemperatureSumList = actualList.stream().map(SensorReadingRollup::getSumTemperature).collect(Collectors.toList());

        assertThat(actualList, hasSize(2));
        assertThat(actualTemperatureSumList, containsInAnyOrder(expectedTemperatureSumList.toArray()));

        //Add same readings again and ensure the sum is doubled
        Observable
                .just(currHourReading1,currHourReading2,nextHourReading1,nextHourReading2)
                .flatMap(sensorReading ->
                        sensorReadingRepo
                                .AddSensorReadingAsync(sensorReading)
                                .subscribeOn(Schedulers.io()))
                .toBlocking()
                .subscribe();

        expectedTemperatureSumList = List.of(6.0,60.0);

        // Sleep to give enough time for syncup
        Thread.sleep(5000);

        actualList =  sensorReadingRepo.GetHourlySensorReadings("1")
                .toList()
                .toBlocking()
                .single();

        actualTemperatureSumList = actualList.stream().map(SensorReadingRollup::getSumTemperature).collect(Collectors.toList());

        assertThat(actualList, hasSize(2));
        assertThat(actualTemperatureSumList, containsInAnyOrder(expectedTemperatureSumList.toArray()));

        syncSubscription.unsubscribe();
    }

    // Helpers
    private SensorReading createSensor(String sensorId){
        var randomStream = new Random().doubles().iterator();
        var sSiteId = String.valueOf(randomStream.next());
        return SensorReading.builder().sensorId(sensorId).siteId(sSiteId).pressure(randomStream.next())
                .temperature(randomStream.next()).unixTimeStamp(Instant.now().toEpochMilli())
                .build();
    }

    private List<SensorReading> addSensorReadings(String sensorId, int numberOfReadings) {
        var sensorReadingRepo = new SensorReadingRepo(client,DATABASE_NAME);

        return Observable
                .range(1,numberOfReadings)
                .concatMap(i-> Observable.just(i).delay(1, TimeUnit.MILLISECONDS))
                .map(i -> createSensor(sensorId))
                .flatMap(sensorReading ->
                        sensorReadingRepo
                                .AddSensorReadingAsync(sensorReading)
                                .subscribeOn(Schedulers.io()))
                .toList()
                .toBlocking()
                .single();
    }
}
