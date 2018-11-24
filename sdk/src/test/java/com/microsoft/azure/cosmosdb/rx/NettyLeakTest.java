package com.microsoft.azure.cosmosdb.rx;

import com.microsoft.azure.cosmosdb.*;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetectorFactory;
import io.netty.util.internal.PlatformDependent;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;
import rx.Observable;
import rx.observers.TestSubscriber;

import java.lang.reflect.Field;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class NettyLeakTest extends TestSuiteBase {
    private final static String DATABASE_ID = getDatabaseId();

    private final static String USE_EXISTING_DB = "cosmosdb.useExistingDB";
    private final static String DB_NAME = "cosmosdb.dbName";
    private final static String COLL_NAME = "activations";

    private static final int INVOCATION_COUNT = 200;


    private Database createdDatabase;
    private DocumentCollection createdCollection;

    private AsyncDocumentClient.Builder clientBuilder;
    private AsyncDocumentClient client;
    private final AtomicInteger counts = new AtomicInteger();

    @Factory(dataProvider = "clientBuilders")
    public NettyLeakTest(AsyncDocumentClient.Builder clientBuilder) {
        this.clientBuilder = clientBuilder;
    }

    @Test(groups = {"simple"}, timeOut = 3600 * 1000, invocationCount = INVOCATION_COUNT, threadPoolSize = 5, invocationTimeOut = 3600 * 1000)
    public void queryDocumentsSortedManyTimes() {
        queryAndLog(true);
    }

    @Test(groups = {"simple"}, timeOut = 3600 * 1000, invocationCount = INVOCATION_COUNT, threadPoolSize = 5, invocationTimeOut = 3600 * 1000)
    public void queryDocumentsUnSortedManyTimes() {
        queryAndLog(false);
    }

    private void queryAndLog(boolean sorted) {
        int size = queryDocuments(sorted).size();
        int count = counts.incrementAndGet();
        if (count % 5 == 0) {
            System.out.printf("%d - %d [%d]%n", count, getDirectMemorySize(), size);
        }

        if (count == INVOCATION_COUNT) {
            assertThat(RecordingLeakDetectorFactory.getLeakCount())
                    .describedAs("reported leak count").isEqualTo(0);
        }
    }

    private List<Document> queryDocuments(boolean sorted) {
        FeedOptions options = new FeedOptions();
        options.setEnableCrossPartitionQuery(true);

        Observable<FeedResponse<Document>> queryObservable = client.queryDocuments(getCollectionLink(), createQuerySpec(sorted), options);
        TestSubscriber<FeedResponse<Document>> testSubscriber = new TestSubscriber<>();
        queryObservable.subscribe(testSubscriber);
        testSubscriber.awaitTerminalEvent(120, TimeUnit.SECONDS);
        testSubscriber.assertNoErrors();
        testSubscriber.assertCompleted();
        return testSubscriber.getOnNextEvents().stream().flatMap(f -> f.getResults().stream()).collect(Collectors.toList());
    }

    private SqlQuerySpec createQuerySpec(boolean sorted) {
        String query = "SELECT TOP 200 * FROM root r WHERE r.start >= @start";
        if (sorted) {
            query += " ORDER BY r.start DESC";
        }
        long start = Instant.now().minus(5, ChronoUnit.DAYS).toEpochMilli();
        SqlParameterCollection coll = new SqlParameterCollection(new SqlParameter("@start", start));
        return new SqlQuerySpec(query, coll);
    }

    @BeforeClass(groups = {"simple"}, timeOut = SETUP_TIMEOUT)
    public void beforeClass() throws Exception {
        RecordingLeakDetectorFactory.register();
        //ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
        client = clientBuilder.build();

        if (useExistingDB()) {
            createdDatabase = getDatabase(client, getDatabaseId());
            createdCollection = getCollection(client, createdDatabase.getSelfLink(), COLL_NAME);
        } else {
            Database d = new Database();
            d.setId(DATABASE_ID);
            createdDatabase = safeCreateDatabase(client, d);
            RequestOptions options = new RequestOptions();
            options.setOfferThroughput(10100);
            createdCollection = createCollection(client, createdDatabase.getId(), getCollectionDefinition(), options);
        }
    }

    @AfterClass(groups = {"simple"}, timeOut = SHUTDOWN_TIMEOUT, alwaysRun = true)
    public void afterClass() {
        if (!useExistingDB()) {
            safeDeleteDatabase(client, createdDatabase.getId());
        }
        safeClose(client);
    }

    /**
     * Gets the direct memory size counter maintained by Netty. If there is a leak then this counter
     * would have an increasing trend
     */
    private static long getDirectMemorySize() {
        try {
            Field field = PlatformDependent.class.getDeclaredField("DIRECT_MEMORY_COUNTER");
            field.setAccessible(true);
            return ((AtomicLong) field.get(null)).longValue();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String getCollectionLink() {
        return Utils.getCollectionNameLink(createdDatabase.getId(), createdCollection.getId());
    }

    private static Database getDatabase(AsyncDocumentClient client, String databaseId) {
        FeedResponse<Database> feedResponsePages = client
                .queryDatabases(new SqlQuerySpec("SELECT * FROM root r WHERE r.id=@id",
                        new SqlParameterCollection(new SqlParameter("@id", databaseId))), null)
                .toBlocking().single();

        if (feedResponsePages.getResults().isEmpty()) {
            throw new RuntimeException("cannot find datatbase " + databaseId);
        }
        return feedResponsePages.getResults().get(0);
    }

    private static DocumentCollection getCollection(AsyncDocumentClient client, String databaseLink,
                                                    String collectionId) {
        FeedResponse<DocumentCollection> feedResponsePages = client
                .queryCollections(databaseLink,
                        new SqlQuerySpec("SELECT * FROM root r WHERE r.id=@id",
                                new SqlParameterCollection(new SqlParameter("@id", collectionId))),
                        null)
                .toBlocking().single();

        if (feedResponsePages.getResults().isEmpty()) {
            throw new RuntimeException("cannot find collection " + collectionId);
        }
        return feedResponsePages.getResults().get(0);
    }

    private static boolean useExistingDB() {
        return Boolean.getBoolean(USE_EXISTING_DB);
    }

    private static String getDatabaseId() {
        if (useExistingDB()) {
            return Objects.requireNonNull(System.getProperty(DB_NAME), "Define db name via system property - " + DB_NAME);
        } else {
            return getDatabaseId(NettyLeakTest.class);
        }
    }

    public static class RecordingLeakDetectorFactory extends ResourceLeakDetectorFactory {
        static final AtomicInteger counter = new AtomicInteger();
        @Override
        public <T> ResourceLeakDetector<T> newResourceLeakDetector(Class<T> resource, int samplingInterval, long maxActive) {
            return new RecordingLeakDetector<T>(counter, resource, samplingInterval);
        }

        public static void register() {
            ResourceLeakDetectorFactory.setResourceLeakDetectorFactory(new RecordingLeakDetectorFactory());
        }

        public static int getLeakCount() {
            return counter.get();
        }
    }

    private static class RecordingLeakDetector<T> extends ResourceLeakDetector<T> {
        private final AtomicInteger counter;
        public RecordingLeakDetector(AtomicInteger counter, Class<?> resourceType, int samplingInterval) {
            super(resourceType, samplingInterval);
            this.counter = counter;
        }

        @Override
        protected void reportTracedLeak(String resourceType, String records) {
            super.reportTracedLeak(resourceType, records);
            counter.incrementAndGet();
        }

        @Override
        protected void reportUntracedLeak(String resourceType) {
            super.reportUntracedLeak(resourceType);
            counter.incrementAndGet();
        }
    }
}
