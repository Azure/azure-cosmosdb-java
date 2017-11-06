/**
 * The MIT License (MIT)
 * Copyright (c) 2016 Microsoft Corporation
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
package com.microsoft.azure.documentdb.rx.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doAnswer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.internal.DocumentServiceResponse;
import com.microsoft.azure.documentdb.internal.HttpConstants;
import com.microsoft.azure.documentdb.rx.AsyncDocumentClient;
import com.microsoft.azure.documentdb.rx.DocumentCrudTest;
import com.microsoft.azure.documentdb.rx.TestSuiteBase;

public class SessionTest extends TestSuiteBase {
    public final static String DATABASE_ID = getDatabaseId(DocumentCrudTest.class);

    private AsyncDocumentClient houseKeepingClient;
    private Database createdDatabase;
    private DocumentCollection createdCollection;

    private RxDocumentClientImpl client1;
    private RxDocumentClientImpl client2;

    private String getCollectionLink() {
        return createdCollection.getSelfLink();
    }

    @BeforeClass(groups = { "simple" }, timeOut = SETUP_TIMEOUT)
    public void beforeClass() {

        houseKeepingClient = createRxWrapperDocumentClient().build();
        Database d = new Database();
        d.setId(DATABASE_ID);
        createdDatabase = safeCreateDatabase(client1, d);
        
        DocumentCollection cl = new DocumentCollection();
        cl.setId(UUID.randomUUID().toString());
        createdCollection = safeCreateCollection(houseKeepingClient, createdDatabase.getId(), cl);
    }

    @AfterClass(groups = { "simple" }, timeOut = SHUTDOWN_TIMEOUT)
    public void afterClass() {
        deleteDatabase(houseKeepingClient, createdDatabase.getId());
        houseKeepingClient.close();
    }

    @BeforeTest
    public void beforeTest() {
        client1 = (RxDocumentClientImpl) createGatewayRxDocumentClient().build();
        client2 = (RxDocumentClientImpl) createGatewayRxDocumentClient().build();
    }

    @AfterTest
    public void afterTest() {
        client1.close();
        client2.close();
    }

    @Test
    public void testSessionConsistency_ReadYourWrites() throws DocumentClientException {
        RxDocumentClientImpl clientUnderTest = Mockito.spy(client1);

        List<String> capturedRequestSessionTokenList = Collections.synchronizedList(new ArrayList<String>());
        List<String> capturedResponseSessionTokenList = Collections.synchronizedList(new ArrayList<String>());

        clientUnderTest.readCollection(getCollectionLink(), null).toBlocking().single();
        clientUnderTest.createDocument(
                getCollectionLink(), new Document(), null, false).toBlocking().single();

        setupSpySession(capturedRequestSessionTokenList, capturedResponseSessionTokenList, clientUnderTest, client1);

        for (int i = 0; i < 10; i++) {

            Document documentCreated = clientUnderTest.createDocument(
                    getCollectionLink(), new Document(), null, false).toBlocking().single().getResource();

            assertThat(capturedRequestSessionTokenList).hasSize(3*i+1);
            assertThat(capturedRequestSessionTokenList.get(3*i+0)).isNotEmpty();

            clientUnderTest.readDocument(documentCreated.getSelfLink(), null).toBlocking().single();

            assertThat(capturedRequestSessionTokenList).hasSize(3*i+2);
            assertThat(capturedRequestSessionTokenList.get(3*i+1)).isNotEmpty();

            clientUnderTest.readDocument(documentCreated.getSelfLink(), null).toBlocking().single();

            assertThat(capturedRequestSessionTokenList).hasSize(3*i+3);
            assertThat(capturedRequestSessionTokenList.get(3*i+2)).isNotEmpty();
        }
    }

    private void setupSpySession(final List<String> capturedRequestSessionTokenList, final List<String> capturedResponseSessionTokenList, 
            RxDocumentClientImpl spyClient, final RxDocumentClientImpl origClient) throws DocumentClientException {

        Mockito.reset(spyClient);
        doAnswer(new Answer<Void>() {
            public Void answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                RxDocumentServiceRequest req = (RxDocumentServiceRequest) args[0];
                DocumentServiceResponse resp = (DocumentServiceResponse) args[1];

                capturedRequestSessionTokenList.add(req.getHeaders().get(HttpConstants.HttpHeaders.SESSION_TOKEN));
                capturedResponseSessionTokenList.add(resp.getResponseHeaders().get(HttpConstants.HttpHeaders.SESSION_TOKEN));

                origClient.captureSessionToken(req, resp);

                return null;
            }})
        .when(spyClient).captureSessionToken(Mockito.any(RxDocumentServiceRequest.class), Mockito.any(DocumentServiceResponse.class));
    }

}
