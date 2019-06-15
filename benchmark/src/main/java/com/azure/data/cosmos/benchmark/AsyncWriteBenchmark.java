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

package com.azure.data.cosmos.benchmark;

import com.azure.data.cosmos.Document;
import com.azure.data.cosmos.ResourceResponse;
import com.codahale.metrics.Timer;
import org.apache.commons.lang3.RandomStringUtils;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.util.UUID;

class AsyncWriteBenchmark extends AsyncBenchmark<ResourceResponse<Document>> {

    private final String uuid;
    private final String dataFieldValue;

    class LatencySubscriber<T> implements Subscriber<T> {

        Timer.Context context;
        Subscriber<T> subscriber;

        LatencySubscriber(Subscriber<T> subscriber) {
            this.subscriber = subscriber;
        }

        @Override
        public void onError(Throwable e) {
            context.stop();
            subscriber.onError(e);
        }

        @Override
        public void onComplete() {
            context.stop();
            subscriber.onComplete();
        }

        @Override
        public void onSubscribe(Subscription s) {

        }

        @Override
        public void onNext(T t) {
            subscriber.onNext(t);
        }
    }

    AsyncWriteBenchmark(Configuration cfg) {
        super(cfg);
        uuid = UUID.randomUUID().toString();
        dataFieldValue = RandomStringUtils.randomAlphabetic(configuration.getDocumentDataFieldSize());
    }

    @Override
    protected void performWorkload(Subscriber<ResourceResponse<Document>> subs, long i) throws InterruptedException {

        String idString = uuid + i;
        Document newDoc = new Document();
        newDoc.id(idString);
        newDoc.set(partitionKey, idString);
        newDoc.set("dataField1", dataFieldValue);
        newDoc.set("dataField2", dataFieldValue);
        newDoc.set("dataField3", dataFieldValue);
        newDoc.set("dataField4", dataFieldValue);
        newDoc.set("dataField5", dataFieldValue);
        Flux<ResourceResponse<Document>> obs = client.createDocument(getCollectionLink(), newDoc, null,
                false);

        concurrencyControlSemaphore.acquire();

        if (configuration.getOperationType() == Configuration.Operation.WriteThroughput) {
            obs.subscribeOn(Schedulers.parallel()).subscribe(subs);
        } else {
            LatencySubscriber<ResourceResponse<Document>> latencySubscriber = new LatencySubscriber<>(
                    subs);
            latencySubscriber.context = latency.time();
            obs.subscribeOn(Schedulers.parallel()).subscribe(latencySubscriber);
        }
    }
}
