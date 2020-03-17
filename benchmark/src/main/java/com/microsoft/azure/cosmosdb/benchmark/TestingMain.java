package com.microsoft.azure.cosmosdb.benchmark;

import com.google.common.collect.Lists;
import com.microsoft.azure.cosmosdb.ConnectionMode;
import com.microsoft.azure.cosmosdb.ConnectionPolicy;
import com.microsoft.azure.cosmosdb.ConsistencyLevel;
import com.microsoft.azure.cosmosdb.Database;
import com.microsoft.azure.cosmosdb.Document;
import com.microsoft.azure.cosmosdb.DocumentCollection;
import com.microsoft.azure.cosmosdb.PartitionKey;
import com.microsoft.azure.cosmosdb.RequestOptions;
import com.microsoft.azure.cosmosdb.ResourceResponse;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class TestingMain {

    private static final Logger log = LoggerFactory.getLogger(TestingMain.class);

    private static final String DATABASES_PATH_SEGMENT = "dbs";
    private static final String COLLECTIONS_PATH_SEGMENT = "colls";
    private static final String DOCUMENTS_PATH_SEGMENT = "docs";

    private static List<DocumentData> documentDataList = new ArrayList<>();

    public static void main(String[] args) throws IOException {
        ConnectionPolicy connectionPolicy = new ConnectionPolicy();
        connectionPolicy.setUsingMultipleWriteLocations(true);
        connectionPolicy.setPreferredLocations(Lists.newArrayList("West US 2"));
        connectionPolicy.setConnectionMode(ConnectionMode.Direct);
        AsyncDocumentClient client = new AsyncDocumentClient
            .Builder()
            .withConsistencyLevel(ConsistencyLevel.BoundedStaleness)
            .withConnectionPolicy(connectionPolicy)
            .withMasterKeyOrResourceToken(
                "")
            .withServiceEndpoint("")
            .build();

        Database database = client.readDatabase(getDatabaseNameLink("test_db"),
            new RequestOptions()).toBlocking().single().getResource();
        log.info("Database read - id {}", database.getId());

        DocumentCollection documentCollection = client.readCollection(getCollectionNameLink(
            "test_db", "test_coll"),
            new RequestOptions()).toBlocking().single().getResource();
        log.info("Collection read - id {}", documentCollection.getId());

        log.info("Reading data from file");
        readDataFromFile(args);
        log.info("Read data from file - size {}", documentDataList.size());

        documentDataList.sort((o1, o2) -> {
            int length = o1.id.length();
            return o1.id.substring(length - 5, length).compareTo(o2.id.substring(length - 5,
                length));
        });

        //  insertData(client, database, documentCollection);
        readDocuments(client, database, documentCollection);
        client.close();
    }

    private static void insertData(AsyncDocumentClient client, Database database,
                                   DocumentCollection documentCollection) {
        for (DocumentData documentData : documentDataList) {
            Document document = createDocument(documentData);
            RequestOptions requestOptions = new RequestOptions();
            requestOptions.setPartitionKey(new PartitionKey(documentData.partitionKey));
            log.info("Creating document with id {}, partitionKey {}", document.getId(),
                documentData.partitionKey);
            Observable<ResourceResponse<Document>> document1 =
                client.createDocument(getCollectionNameLink(database.getId(),
                    documentCollection.getId()), document, null, true);
            Document resource = document1.toBlocking().single().getResource();
            log.info("Document created with id {} and parition key {}", resource.getId(),
                resource.get("my_pk"));
        }
    }

    private static Document createDocument(DocumentData documentData) {
        Document document = new Document();
        document.setId(documentData.id);
        document.set("my_pk", documentData.partitionKey);
        document.set(documentData.partitionKey, documentData.partitionKey);
        document.set("someProperty1", "Random text property");
        document.set("someProperty2", "Random text property");
        document.set("someProperty3", "Random text property");
        document.set("someProperty4", "Random text property");
        document.set("someProperty5", "Random text property");
        document.set("someProperty6", "Random text property");
        document.set("someProperty7", "Random text property");
        return document;
    }

    private static void readDataFromFile(String[] args) throws IOException {
        String fileName = "id_partionkey_list.txt";
        File file = null;
        if (args.length > 0) {
            fileName = args[0];
            file = new File(fileName);
        } else {
            ClassLoader classLoader = TestingMain.class.getClassLoader();
            URL resource = classLoader.getResource(fileName);
            assert resource != null;
            file = new File(resource.getFile());
        }
        //File is found
        log.info("File Found : " + file.exists());

        //Read File Content
        String content = new String(Files.readAllBytes(file.toPath()));

        Set<DocumentData> documentDataSet = new HashSet<>();
        String[] split = content.split("\n");
        log.info("Contents size {}", split.length);
        for (String s : split) {
            String[] split1 = s.split(",");
            String id = split1[0].trim().substring(3);
            String partitionKey = split1[1].trim().substring(13);
            DocumentData documentData = new DocumentData(id, partitionKey);
            documentDataSet.add(documentData);
        }
        log.info("Total elements added {}", documentDataSet.size());
        documentDataList.addAll(documentDataSet);
    }

    private static void readDocuments(AsyncDocumentClient client, Database database,
                                      DocumentCollection documentCollection) {
        for (DocumentData documentData : documentDataList) {
            RequestOptions requestOptions = new RequestOptions();
            requestOptions.setPartitionKey(new PartitionKey(documentData.partitionKey));
            String documentNameLink = getDocumentNameLink(database.getId(),
                documentCollection.getId(), documentData.id);
            long startTime = System.currentTimeMillis();
            Document document =
                client.readDocument(documentNameLink, requestOptions).toBlocking().single().getResource();
            log.info("Time taken {}", (System.currentTimeMillis() - startTime));
            log.info("Document read id {} with - id {}, partition key {}",
                document.getId(), documentData.id, documentData.partitionKey);
        }
    }

    public static String getDatabaseNameLink(String databaseId) {
        return DATABASES_PATH_SEGMENT + "/" + databaseId;
    }

    public static String getCollectionNameLink(String databaseId, String collectionId) {

        return DATABASES_PATH_SEGMENT + "/" + databaseId + "/" + COLLECTIONS_PATH_SEGMENT + "/" + collectionId;
    }

    public static String getDocumentNameLink(String databaseId, String collectionId, String docId) {

        return DATABASES_PATH_SEGMENT + "/" + databaseId + "/" + COLLECTIONS_PATH_SEGMENT + "/" + collectionId + "/" + DOCUMENTS_PATH_SEGMENT + "/" + docId;
    }

    private static class DocumentData {
        String id;
        String partitionKey;

        DocumentData(String id, String partitionKey) {
            this.id = id;
            this.partitionKey = partitionKey;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DocumentData that = (DocumentData)o;
            return Objects.equals(id, that.id) &&
                Objects.equals(partitionKey, that.partitionKey);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, partitionKey);
        }
    }
}
