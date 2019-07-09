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
package com.azure.data.cosmos.examples;

import com.azure.data.cosmos.*;
import reactor.core.publisher.Mono;

import java.io.IOException;

public class HelloWorldDemo {
    public static void main(String[] args) {
        new HelloWorldDemo().runDemo();
    }

    void runDemo() {
        // Create a new CosmosClient via the builder
        // It only requires endpoint and key, but other useful settings are available
        CosmosClient client = CosmosClient.builder()
            .endpoint("<YOUR ENDPOINT HERE>")
            .key("<YOUR KEY HERE>")
            .build();

        // Get a reference to the container
        // This will create (or read) a database and its container.
        CosmosContainer container = client.createDatabaseIfNotExists("contoso-travel")
            // TIP: Our APIs are Reactor Core based, so try to chain your calls
            .flatMap(response -> response.database()
                    .createContainerIfNotExists("passengers", "/id"))
            .flatMap(response -> Mono.just(response.container()))
            .block(); // Blocking for demo purposes (avoid doing this in production unless you must)

        // Create an item ✨
        container.createItem(new Passenger("carla.davis@outlook.com", "Carla Davis", "SEA", "IND"))
            .flatMap(response -> {
                System.out.println("Created item: " + response.properties().toJson());
                // Read that item 👓
                return response.item().read();
            })
            .flatMap(response -> {
                System.out.println("Read item: " + response.properties().toJson());
                // Replace that item 🔁
                try {
                    Passenger p = response.properties().getObject(Passenger.class);
                    p.setDestination("SFO");
                    return response.item().replace(p);
                } catch (IOException e) {
                    System.err.println(e);
                    throw new RuntimeException("Couldn't replace item", e);
                }
            })
            // delete that item 💣
            .flatMap(response -> response.item().delete())
            .block(); // Blocking for demo purposes (avoid doing this in production unless you must)
    }

    // Just a random object for demo's sake
    public class Passenger {
        String id;
        String name;
        String destination;
        String source;

        public Passenger(String id, String name, String destination, String source) {
            this.id = id;
            this.name = name;
            this.destination = destination;
            this.source = source;
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
