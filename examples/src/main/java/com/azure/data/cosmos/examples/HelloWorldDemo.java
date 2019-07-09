package com.azure.data.cosmos.examples;

import com.azure.data.cosmos.CosmosClient;
import com.azure.data.cosmos.CosmosContainer;
import com.azure.data.cosmos.CosmosDatabase;
import com.azure.data.cosmos.CosmosItemProperties;

public class HelloWorldDemo {
    public static void main(String[] args) {
        // Create a new CosmosClient via the builder
        // It only requires endpoint and key, but other useful settings are available
        CosmosClient client = CosmosClient.builder()
                .endpoint("<YOUR ENDPOINT HERE>")
                .key("<YOUR KEY HERE>")
                .build();

        client.createDatabaseIfNotExists("contoso-travel")
                .flatMap(response -> response.database()
                        .createContainerIfNotExists("passengers", "/id"))
                .flatMap(response -> response.container()
                        .createItem(new Passenger("carla.davis@outlook.com", "Carla Davis", "SEA", "IND"))
    }

    public class Passenger {
        String id;
        String name;
        String destination;
        String source;

        public Passenger(String id, String name, String destination, String source) {
            this.id = id; this.name = name; this.destination = destination; this.source = source;
        }
        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getDestination() {
            return destination;
        }

        public void setDestination(String destination) {
            this.destination = destination;
        }

        public String getSource() {
            return source;
        }

        public void setSource(String source) {
            this.source = source;
        }
    }
}
