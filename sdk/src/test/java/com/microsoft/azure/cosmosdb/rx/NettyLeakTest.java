package com.microsoft.azure.cosmosdb.rx;

import com.microsoft.azure.cosmosdb.*;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetectorFactory;
import io.netty.util.internal.PlatformDependent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;
import rx.Observable;
import rx.observers.TestSubscriber;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class NettyLeakTest extends TestSuiteBase {
    private static final Logger log = LoggerFactory.getLogger(NettyLeakTest.class);
    private final static String DATABASE_ID = getDatabaseId();

    private final static String USE_EXISTING_DB = "cosmosdb.useExistingDB";
    private final static String DB_NAME = "cosmosdb.dbName";
    private final static String COLL_NAME = "activations";

    private static final int INVOCATION_COUNT = 200;
    private static final long ATTR_START = 1542890298107L;
    private static final int START_RANGE = (int)TimeUnit.DAYS.toMillis(7);

    private static final int TEST_DATA_COUNT = 50000;
    private static final int INSERT_BATCH_SIZE = 1000;
    private static final long TEST_TIMEOUT = 3600 * 1000;


    private Random rnd = new Random(42);
    private Database createdDatabase;
    private DocumentCollection createdCollection;

    private AsyncDocumentClient.Builder clientBuilder;
    private AsyncDocumentClient client;
    private final AtomicInteger counts = new AtomicInteger();

    @Factory(dataProvider = "clientBuilders")
    public NettyLeakTest(AsyncDocumentClient.Builder clientBuilder) {
        this.clientBuilder = clientBuilder;
    }

    @Test(groups = {"simple"}, timeOut = TEST_TIMEOUT, invocationCount = INVOCATION_COUNT, threadPoolSize = 5, invocationTimeOut = TEST_TIMEOUT)
    public void queryDocumentsSortedManyTimes() {
        queryAndLog(true);
    }

    @Test(groups = {"simple"}, timeOut = TEST_TIMEOUT, invocationCount = INVOCATION_COUNT, threadPoolSize = 5, invocationTimeOut = TEST_TIMEOUT)
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
        long start = ATTR_START + START_RANGE / 2;
        SqlParameterCollection coll = new SqlParameterCollection(new SqlParameter("@start", start));
        return new SqlQuerySpec(query, coll);
    }

    @BeforeClass(groups = {"simple"}, timeOut = TEST_TIMEOUT)
    public void beforeClass() throws Exception {
        RecordingLeakDetectorFactory.register();
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
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
            createdCollection = createCollection(client, createdDatabase.getId(), getTestCollectionDefinition(), options);
            log.info("Seeding database with {} records", TEST_DATA_COUNT);
            bulkInsertInBatch(client, TEST_DATA_COUNT);
            log.info("Created db with {} records", TEST_DATA_COUNT);
        }
    }

    @AfterClass(groups = {"simple"}, timeOut = SHUTDOWN_TIMEOUT, alwaysRun = true)
    public void afterClass() {
        if (!useExistingDB()) {
            //safeDeleteDatabase(client, createdDatabase.getId());
        }
        safeClose(client);
    }

    private String getCollectionLink() {
        return Utils.getCollectionNameLink(createdDatabase.getId(), createdCollection.getId());
    }

    /**
     * Creates a collection with 'id' as partition key and an ordered index on 'start' key
     */
    private DocumentCollection getTestCollectionDefinition() {
        PartitionKeyDefinition partitionKeyDef = new PartitionKeyDefinition();
        ArrayList<String> paths = new ArrayList<>();
        paths.add("/id");
        partitionKeyDef.setPaths(paths);
        IndexingPolicy indexingPolicy = new IndexingPolicy();

        Collection<IncludedPath> includedPaths = new ArrayList<>();
        IncludedPath includedPath = new IncludedPath();
        includedPath.setPath("/start/?");
        Collection<Index> indexes = new ArrayList<>();

        Index numberIndex = Index.Range(DataType.Number);
        numberIndex.set("precision", -1);
        indexes.add(numberIndex);
        includedPath.setIndexes(indexes);
        includedPaths.add(includedPath);
        indexingPolicy.setIncludedPaths(includedPaths);

        List<ExcludedPath> excludedPaths = new ArrayList<>();
        ExcludedPath excludedPath = new ExcludedPath();
        excludedPath.setPath("/");
        excludedPaths.add(excludedPath);
        indexingPolicy.setExcludedPaths(excludedPaths);

        DocumentCollection collectionDefinition = new DocumentCollection();
        collectionDefinition.setIndexingPolicy(indexingPolicy);
        collectionDefinition.setId(COLL_NAME);
        collectionDefinition.setPartitionKey(partitionKeyDef);

        return collectionDefinition;
    }

    private void bulkInsertInBatch(AsyncDocumentClient client, int count) {
        int batchCount = count / 1000;
        for (int i = 0; i < batchCount; i++) {
            bulkInsert(client, INSERT_BATCH_SIZE);
            int progress = (int)((i + 1) * INSERT_BATCH_SIZE / (double) count * 100);
            log.info("Inserted batch of {}. {}% done", INSERT_BATCH_SIZE, progress);
        }
    }

    private List<Document> bulkInsert(AsyncDocumentClient client, int count) {

        List<Observable<ResourceResponse<Document>>> result = new ArrayList<>(count);

        for (int i = 0; i < count; i++) {
            Document docDefinition = getDocumentDefinition(i);
            Observable<ResourceResponse<Document>> obs = client.createDocument(getCollectionLink(), docDefinition, null, false);
            result.add(obs);
        }

        return Observable.merge(result, 100).
                map(resp -> resp.getResource())
                .toList().toBlocking().single();
    }

    private Document getDocumentDefinition(int i) {
        long start = ATTR_START + rnd.nextInt(START_RANGE);
        String template = "{" +
                "  'id': '%s'," +
                "  '_c': {" +
                "    'deleteLogs': true," +
                "    'nspath': 'whisk.system/invokerHealthTestAction0'" +
                "  }," +
                "  'activationId': '8658e536d85244b098e536d852e4b026'," +
                "  'annotations': [" +
                "    {" +
                "      'key': 'limits'," +
                "      'value': {" +
                "        'concurrency': 1," +
                "        'logs': 10," +
                "        'memory': 256," +
                "        'timeout': 60000" +
                "      }" +
                "    }," +
                "    {" +
                "      'key': 'path'," +
                "      'value': 'whisk.system/invokerHealthTestAction0'" +
                "    }," +
                "    {" +
                "      'key': 'kind'," +
                "      'value': 'nodejs:6'" +
                "    }," +
                "    {" +
                "      'key': 'waitTime'," +
                "      'value': 478995" +
                "    }" +
                "  ]," +
                "  'duration': 14," +
                "  'end': 1542890298107," +
                "  'entityType': 'activation'," +
                "  'logs': []," +
                "  'name': 'invokerHealthTestAction0'," +
                "  'namespace': 'whisk.system'," +
                "  'publish': false," +
                "  'response': {" +
                "    'result': {}," +
                "    'statusCode': 0" +
                "  }," +
                "  'start': " + start + "," +
                "  'subject': 'whisk.system'," +
                "  'updated': 1542890298113," +
                "  'version': '0.0.1'," +
                "}";
        String json = String.format(template, UUID.randomUUID().toString());
        return new Document(json);
    }

    //~------------------------------------< CosmosDB utility methods >

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

    //~----------------------------------< Netty Utility classes >


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

    public static class RecordingLeakDetectorFactory extends ResourceLeakDetectorFactory {
        static final AtomicInteger counter = new AtomicInteger();

        @Override
        public <T> ResourceLeakDetector<T> newResourceLeakDetector(Class<T> resource, int samplingInterval, long maxActive) {
            return new RecordingLeakDetector<>(counter, resource, samplingInterval);
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

        RecordingLeakDetector(AtomicInteger counter, Class<?> resourceType, int samplingInterval) {
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
