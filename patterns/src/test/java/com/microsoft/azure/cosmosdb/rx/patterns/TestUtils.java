package com.microsoft.azure.cosmosdb.rx.patterns;
import com.microsoft.azure.cosmosdb.*;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class TestUtils {
    public static final int HTTP_STATUS_CODE_CREATED = 201;
    public static final int HTTP_STATUS_CODE_NO_CONTENT = 204;
    private static final Logger LOGGER = LoggerFactory.getLogger(TestUtils.class);

    public static void safeclean(AsyncDocumentClient client, String databaseName) {
        if (client != null && databaseName!=null) {
            try {
                client.deleteDatabase(UriFactory.createDatabaseUri(databaseName), null).toBlocking().single();
            } catch (Exception e) {
                LOGGER.error("safe clean error",e);
            }
            client.close();
        }
    }

    public static String getDatabaseName(Class<?> klass) {
        return String.format("%s", klass.getName());

    }

    public static void cleanUpDatabase(AsyncDocumentClient client, String databaseName) {

        LOGGER.info("cleanUpDatabase invoked");

        try {
            var feedResponsePages = client
                    .queryDatabases(new SqlQuerySpec("SELECT * FROM root r WHERE r.id=@id",
                            new SqlParameterCollection(new SqlParameter("@id", databaseName))), null)
                    .toList().toBlocking().single();

            if (!feedResponsePages.get(0).getResults().isEmpty()) {
                Database res = feedResponsePages.get(0).getResults().get(0);
                LOGGER.info("deleting a database " + feedResponsePages.get(0));
                client.deleteDatabase("dbs/" + res.getId(), null).toBlocking().single();
            }
        } catch (Exception e) {
            LOGGER.error("safe clean error",e);
        }

    }
}
