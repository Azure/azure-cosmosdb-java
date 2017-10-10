/**
 * The MIT License (MIT)
 * Copyright (c) 2017 Microsoft Corporation
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
package com.microsoft.azure.documentdb.rx;

import java.util.List;

import org.json.JSONObject;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.Offer;
import com.microsoft.azure.documentdb.ResourceResponse;

import rx.Observable;

public class OfferReadReplaceTest extends TestSuiteBase {

    public final static String DATABASE_ID = getDatabaseId(OfferReadReplaceTest.class);

    private static AsyncDocumentClient houseKeepingClient;
    private static Database createdDatabase;
    private static DocumentCollection createdCollection;

    private AsyncDocumentClient.Builder clientBuilder;
    private AsyncDocumentClient client;

    @Factory(dataProvider = "clientBuilders")
    public OfferReadReplaceTest(AsyncDocumentClient.Builder clientBuilder) {
        this.clientBuilder = clientBuilder;
    }

    @Test(groups = { "simple" }, timeOut = TIMEOUT)
    public void readAndReplaceOffer() throws Exception {

        client.readOffers(null).toBlocking().subscribe((offersFeed) -> {
            try {
                int i;
                List<Offer> offers = offersFeed.getResults();
                for (i = 0; i < offers.size(); i++) {
                    if (offers.get(i).getOfferResourceId().equals(createdCollection.getResourceId())) {
                        break;
                    }
                }

                Offer rOffer = client.readOffer(offers.get(i).getSelfLink()).toBlocking().single().getResource();
                int oldThroughput = rOffer.getContent().getInt("offerThroughput");
                
                Observable<ResourceResponse<Offer>> readObservable = client.readOffer(offers.get(i).getSelfLink());

                // validate offer read
                ResourceResponseValidator<Offer> validatorForRead = new ResourceResponseValidator.Builder<Offer>()
                        .withOfferThroughput(oldThroughput)
                        .notNullEtag()
                        .build();

                validateSuccess(readObservable, validatorForRead);

                // update offer
                int newThroughput = oldThroughput + 100;
                
                JSONObject modifiedContent = new JSONObject(
                        "{\"offerThroughput\":" + new Integer(newThroughput).toString() + ",\"offerIsRUPerMinuteThroughputEnabled\":false}");
                offers.get(i).setContent(modifiedContent);
                Observable<ResourceResponse<Offer>> replaceObservable = client.replaceOffer(offers.get(i));

                // validate offer replace
                ResourceResponseValidator<Offer> validatorForReplace = new ResourceResponseValidator.Builder<Offer>()
                        .withOfferThroughput(newThroughput)
                        .notNullEtag()
                        .build();
                
                validateSuccess(replaceObservable, validatorForReplace);
                
            } catch (Exception e) {
                e.printStackTrace();
            }

        });
    }
    
    @BeforeClass(groups = { "simple" }, timeOut = SETUP_TIMEOUT)
    public void beforeClass() {
        this.client = this.clientBuilder.build();
    }

    @AfterClass(groups = { "simple" }, timeOut = SHUTDOWN_TIMEOUT)
    public void afterClass() {
        this.client.close();
    }

    @BeforeSuite(groups = { "simple" }, timeOut = SETUP_TIMEOUT)
    public static void beforeSuite() {
        houseKeepingClient = createGatewayRxDocumentClient().build();
        Database d = new Database();
        d.setId(DATABASE_ID);
        createdDatabase = safeCreateDatabase(houseKeepingClient, d);
        createdCollection = safeCreateCollection(houseKeepingClient, createdDatabase.getId(),
                getCollectionDefinition());
    }

    @AfterSuite(groups = { "simple" }, timeOut = SETUP_TIMEOUT)
    public static void afterSuite() {

        deleteDatabase(houseKeepingClient, createdDatabase.getId());
        houseKeepingClient.close();
    }

}
