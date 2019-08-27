/*
 * The MIT License (MIT)
 * Copyright (c) 2018 Microsoft Corporation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.microsoft.azure.cosmosdb.rx;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.cosmosdb.Database;
import com.microsoft.azure.cosmosdb.Document;
import com.microsoft.azure.cosmosdb.DocumentClientException;
import com.microsoft.azure.cosmosdb.DocumentCollection;
import com.microsoft.azure.cosmosdb.FeedOptions;
import com.microsoft.azure.cosmosdb.FeedResponse;
import com.microsoft.azure.cosmosdb.JsonSerializable;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.Protocol;
import com.microsoft.azure.cosmosdb.rx.internal.Utils.ValueHolder;
import com.microsoft.azure.cosmosdb.rx.internal.query.UnorderedDistinctMap;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;
import rx.Observable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class DistinctQueryTests extends TestSuiteBase {
    private final String FIELD = "name";
    private Database createdDatabase;
    private DocumentCollection createdCollection;
    private ArrayList<Document> docs = new ArrayList<>();
    private AsyncDocumentClient client;

    @Factory(dataProvider = "clientBuildersWithDirect")
    public DistinctQueryTests(AsyncDocumentClient.Builder clientBuilder) {
        super(clientBuilder);
    }

    private static String GetRandomName(Random rand) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("name_" + rand.nextInt(100));

        return stringBuilder.toString();
    }

    private static City GetRandomCity(Random rand) {
        int index = rand.nextInt(3);
        switch (index) {
            case 0:
                return City.LosAngeles;
            case 1:
                return City.NewYork;
            case 2:
                return City.Seattle;
        }

        return City.LosAngeles;
    }

    private static double GetRandomIncome(Random rand) {
        return rand.nextDouble() * Double.MAX_VALUE;
    }

    private static int GetRandomAge(Random rand) {
        return rand.nextInt(100);
    }

    private Pet GetRandomPet(Random rand) {
        String name = GetRandomName(rand);
        int age = GetRandomAge(rand);
        return new Pet(name, age);
    }

    public Person GetRandomPerson(Random rand) {
        String name = GetRandomName(rand);
        City city = GetRandomCity(rand);
        double income = GetRandomIncome(rand);
        List<Person> people = new ArrayList<Person>();
        if (rand.nextInt(10) % 10 == 0) {
            for (int i = 0; i < rand.nextInt(5); i++) {
                people.add(GetRandomPerson(rand));
            }
        }

        int age = GetRandomAge(rand);
        Pet pet = GetRandomPet(rand);
        UUID guid = UUID.randomUUID();
        Person p = new Person(name, city, income, people, age, pet, guid);
        return p;
    }

    @Test(groups = {"simple"}, timeOut = TIMEOUT, dataProvider = "queryMetricsArgProvider")
    public void queryDocuments(boolean qmEnabled) {
        String query = "SELECT DISTINCT c.name from c";
        FeedOptions options = new FeedOptions();
        options.setMaxItemCount(5);
        options.setEnableCrossPartitionQuery(true);
        options.setPopulateQueryMetrics(qmEnabled);
        options.setMaxDegreeOfParallelism(2);
        Observable<FeedResponse<Document>> queryObservable = client.queryDocuments(createdCollection.getSelfLink(),
                query, options);

        List<Object> distinctList = docs.stream().map(d -> d.get(FIELD)).distinct().collect(Collectors.toList());
        FeedResponseListValidator<Document> validator =
                new FeedResponseListValidator.Builder<Document>().totalSize(distinctList.size()).allPagesSatisfy(new FeedResponseValidator.Builder<Document>().requestChargeGreaterThanOrEqualTo(1.0).build())
                        .hasValidQueryMetrics(qmEnabled)
                        .build();

        try {
            validateQuerySuccess(queryObservable, validator, TIMEOUT);
        } catch (Throwable error) {
            if (this.clientBuilder().configs.getProtocol() == Protocol.Tcp) {
                String message = String.format("Direct TCP test failure: desiredConsistencyLevel=%s",
                        this.clientBuilder().desiredConsistencyLevel);
                logger.info(message, error);
                throw new SkipException(message, error);
            }
            throw error;
        }
    }

    @Test(groups = {"long"}, timeOut = TIMEOUT)
    public void queryDistinctDocuments() {

        List<String> queries = Arrays.asList(
                // basic distinct queries
                "SELECT %s VALUE null",
                "SELECT %s VALUE false",
                "SELECT %s VALUE true",
                "SELECT %s VALUE 1",
                "SELECT %s VALUE 'a'",
                "SELECT %s VALUE [null, true, false, 1, 'a']",
                "SELECT %s false AS p",
                "SELECT %s 1 AS p",
                "SELECT %s 'a' AS p",

                "SELECT %s VALUE null FROM c",
                "SELECT %s VALUE false FROM c",
                "SELECT %s VALUE 1 FROM c",
                "SELECT %s VALUE 'a' FROM c",
                "SELECT %s null AS p FROM c",
                "SELECT %s false AS p FROM c",
                "SELECT %s 1 AS p FROM c",
                "SELECT %s 'a' AS p FROM c",

                // number value distinct queries
                "SELECT %s VALUE c.income from c",
                "SELECT %s VALUE c.age from c",
                "SELECT %s c.income, c.income AS income2 from c",
                "SELECT %s c.income, c.age from c",

                // string value distinct queries
                "SELECT %s  c.name from c",
                "SELECT %s VALUE c.city from c",
                "SELECT %s VALUE c.partitionKey from c",
                "SELECT %s c.name, c.name AS name2 from c",
                "SELECT %s c.name, c.city from c",

                // array distinct queries
                "SELECT %s c.children from c",
                "SELECT %s c.children, c.children AS children2 from c",

                // object value distinct queries
                "SELECT %s VALUE c.pet from c",
                "SELECT %s c.pet, c.pet AS pet2 from c",

                // scalar expressions distinct query
                "SELECT %s VALUE ABS(c.age) FROM c",
                "SELECT %s VALUE LEFT(c.name, 1) FROM c",
                "SELECT %s VALUE c.name || ', ' || (c.city ?? '') FROM c",
                "SELECT %s VALUE ARRAY_LENGTH(c.children) FROM c",
                "SELECT %s VALUE IS_DEFINED(c.city) FROM c",
                "SELECT %s VALUE (c.children[0].age ?? 0) + (c.children[1].age ?? 0) FROM c",

                // distinct queries with order by : Value order by queries are not supported yet 
                "SELECT %s  c.name FROM c ORDER BY c.name ASC",
                "SELECT %s  c.age FROM c ORDER BY c.age",
                "SELECT %s  c.city FROM c ORDER BY c.city",
                "SELECT %s  c.city FROM c ORDER BY c.age",
                "SELECT %s  LEFT(c.name, 1) FROM c ORDER BY c.name",

                // distinct queries with top and no matching order by
                "SELECT %s TOP 2147483647 VALUE c.age FROM c",

                // distinct queries with top and  matching order by
                "SELECT %s TOP 2147483647  c.age FROM c ORDER BY c.age",

                // distinct queries with aggregates
                "SELECT %s VALUE MAX(c.age) FROM c",

                // distinct queries with joins
                "SELECT %s VALUE c.age FROM p JOIN c IN p.children",
                "SELECT %s p.age AS ParentAge, c.age ChildAge FROM p JOIN c IN p.children",
                "SELECT %s VALUE c.name FROM p JOIN c IN p.children",
                "SELECT %s p.name AS ParentName, c.name ChildName FROM p JOIN c IN p.children",

                // distinct queries in subqueries
                "SELECT %s r.age, s FROM r JOIN (SELECT DISTINCT VALUE c FROM (SELECT 1 a) c) s WHERE r.age > 25",
                "SELECT %s p.name, p.age FROM (SELECT DISTINCT * FROM r) p WHERE p.age > 25",

                // distinct queries in scalar subqeries
                "SELECT %s p.name, (SELECT DISTINCT VALUE p.age) AS Age FROM p",
                "SELECT %s p.name, p.age FROM p WHERE (SELECT DISTINCT VALUE LEFT(p.name, 1)) > 'A' AND (SELECT " +
                        "DISTINCT VALUE p.age) > 21",
                "SELECT %s p.name, (SELECT DISTINCT VALUE p.age) AS Age FROM p WHERE (SELECT DISTINCT VALUE p.name) >" +
                        " 'A' OR (SELECT DISTINCT VALUE p.age) > 21",

                //   select *
                "SELECT %s * FROM c"
        );

        for (String query : queries) {
            FeedOptions options = new FeedOptions();
            options.setMaxItemCount(5);
            options.setEnableCrossPartitionQuery(true);
            options.setMaxDegreeOfParallelism(2);

            List<Document> documentsFromWithDistinct = new ArrayList<>();
            List<Document> documentsFromWithoutDistinct = new ArrayList<>();

            final String queryWithDistinct = String.format(query, "DISTINCT");
            final String queryWithoutDistinct = String.format(query, "");


            Observable<FeedResponse<Document>> queryObservable = client.queryDocuments(createdCollection.getSelfLink(),
                    queryWithoutDistinct, options);

            Iterator<FeedResponse<Document>> iterator = queryObservable.toBlocking().getIterator();
            ValueHolder<String> outHash = new ValueHolder<>();
            UnorderedDistinctMap distinctMap = new UnorderedDistinctMap();

            while (iterator.hasNext()) {
                FeedResponse<Document> next = iterator.next();
                for (Document document : next.getResults()) {
                    if (distinctMap.add(document, outHash)) {
                        documentsFromWithoutDistinct.add(document);
                    }
                }
            }

            Observable<FeedResponse<Document>> queryObservableWithDistinct =
                    client.queryDocuments(createdCollection.getSelfLink(),
                            queryWithDistinct, options);
            iterator = queryObservableWithDistinct.toBlocking().getIterator();

            while (iterator.hasNext()) {
                FeedResponse<Document> next = iterator.next();
                documentsFromWithDistinct.addAll(next.getResults());
            }

            assertThat(documentsFromWithDistinct.size()).isEqualTo(documentsFromWithoutDistinct.size());
        }

    }

    @Test(groups = {"simple"}, timeOut = TIMEOUT * 10)
    public void queryDocumentsWithUnorderedDistinctContinuationTokens() {
        String query = "SELECT DISTINCT c.name from c";
        // Query with unordered distinct continuation token is not supported
        this.queryWithContinuationTokensAndPageSizes(query, new int[] {1});
    }

    private void queryWithContinuationTokensAndPageSizes(String query, int[] pageSizes) {
        for (int pageSize : pageSizes) {
            List<Document> receivedDocuments = this.queryWithContinuationTokens(query, pageSize);
        }
    }

    private List<Document> queryWithContinuationTokens(String query, int pageSize) {
        String requestContinuation = null;
        List<String> continuationTokens = new ArrayList<String>();
        List<Document> receivedDocuments = new ArrayList<Document>();

        do {
            FeedOptions options = new FeedOptions();
            options.setMaxItemCount(pageSize);
            options.setEnableCrossPartitionQuery(true);
            options.setMaxDegreeOfParallelism(2);
            options.setRequestContinuation(requestContinuation);
            Observable<FeedResponse<Document>> queryObservable =
                    client.queryDocuments(createdCollection.getSelfLink(), query, options);

            Observable<FeedResponse<Document>> firstPageObservable = queryObservable.first();
            VerboseTestSubscriber<FeedResponse<Document>> testSubscriber = new VerboseTestSubscriber<>();
            firstPageObservable.subscribe(testSubscriber);
            testSubscriber.awaitTerminalEvent(TIMEOUT, TimeUnit.MILLISECONDS);
            if (requestContinuation != null) {
                // Unordered distinct queries doesnt support continuation tokens
                testSubscriber.assertError(DocumentClientException.class);
                break;
            } else {
                testSubscriber.assertCompleted();
                FeedResponse<Document> firstPage = testSubscriber.getOnNextEvents().get(0);
                requestContinuation = firstPage.getResponseContinuation();
                receivedDocuments.addAll(firstPage.getResults());
                continuationTokens.add(requestContinuation);
            }
        } while (requestContinuation != null);

        return receivedDocuments;
    }

    public void bulkInsert(AsyncDocumentClient client) {
        generateTestData();

        for (Document doc : docs) {
            createDocument(client, createdDatabase.getId(), createdCollection.getId(), doc);
        }
    }

    public void generateTestData() {

        Random rand = new Random();
        ObjectMapper mapper = new ObjectMapper();
        for (int i = 0; i < 40; i++) {
            Person person = GetRandomPerson(rand);
            for (int j = 0; j < rand.nextInt(4); j++) {
                try {
                    docs.add(new Document(mapper.writeValueAsString(person)));
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @AfterClass(groups = {"simple"}, timeOut = SHUTDOWN_TIMEOUT, alwaysRun = true)
    public void afterClass() {
        safeClose(client);
    }

    @BeforeClass(groups = {"simple"}, timeOut = SETUP_TIMEOUT)
    public void beforeClass() throws Exception {
        client = this.clientBuilder().build();
        createdDatabase = SHARED_DATABASE;
        createdCollection = SHARED_MULTI_PARTITION_COLLECTION;
        truncateCollection(SHARED_MULTI_PARTITION_COLLECTION);

        bulkInsert(client);

        waitIfNeededForReplicasToCatchUp(clientBuilder());
    }


    public enum City {
        NewYork,
        LosAngeles,
        Seattle
    }

    public final class Pet extends JsonSerializable {
        @JsonProperty("name")
        public String name;

        @JsonProperty("age")
        public int age;

        public Pet(String name, int age) {
            this.name = name;
            this.age = age;
        }
    }

    public final class Person extends JsonSerializable {
        @JsonProperty("name")
        public String Name;

        @JsonProperty("city")
        public City City;

        @JsonProperty("income")
        public double Income;

        @JsonProperty("children")
        public List<Person> Children;

        @JsonProperty("age")
        public int Age;

        @JsonProperty("pet")
        public Pet Pet;

        @JsonProperty("guid")
        public UUID Guid;

        public Person(String name, City city, double income, List<Person> children, int age, Pet pet, UUID guid) {
            this.Name = name;
            this.City = city;
            this.Income = income;
            this.Children = children;
            this.Age = age;
            this.Pet = pet;
            this.Guid = guid;
        }

    }
}
