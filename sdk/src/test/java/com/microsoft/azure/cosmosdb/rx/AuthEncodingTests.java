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

import com.microsoft.azure.cosmosdb.ConnectionMode;
import com.microsoft.azure.cosmosdb.Document;
import com.microsoft.azure.cosmosdb.DocumentCollection;
import com.microsoft.azure.cosmosdb.internal.HttpConstants;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;
import com.microsoft.azure.cosmosdb.rx.TestSuiteBase;
import com.microsoft.azure.cosmosdb.rx.internal.SpyClientUnderTestFactory;

import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class AuthEncodingTests extends TestSuiteBase {
    protected static final int TIMEOUT = 20000;

    private DocumentCollection createdCollection;
    private SpyClientUnderTestFactory.SpyBaseClass<HttpClientRequest<ByteBuf>> spyClient;
    private AsyncDocumentClient houseKeepingClient;
    private ConnectionMode connectionMode;
    private static final Pattern URL_PERCENT_ENCODING = Pattern.compile("%[0-9A-F]{2}");

    @Factory(dataProvider = "clientBuildersWithDirectSession")
    public AuthEncodingTests(AsyncDocumentClient.Builder clientBuilder) {
        super(clientBuilder);
        this.subscriberValidationTimeout = TIMEOUT;
    }

    @BeforeClass(groups = { "simple" }, timeOut = SETUP_TIMEOUT)
    public void beforeClass() throws Exception {
        createdCollection = SHARED_MULTI_PARTITION_COLLECTION;
        houseKeepingClient = clientBuilder().build();
        connectionMode = houseKeepingClient.getConnectionPolicy().getConnectionMode();

        if (connectionMode == ConnectionMode.Direct) {
            spyClient = SpyClientUnderTestFactory.createDirectHttpsClientUnderTest(clientBuilder());
        } else {
            spyClient = SpyClientUnderTestFactory.createClientUnderTest(clientBuilder());
        }
    }

    @AfterClass(groups = { "simple" }, timeOut = SHUTDOWN_TIMEOUT, alwaysRun = true)
    public void afterClass() {
        safeClose(houseKeepingClient);
        safeClose(spyClient);
    }

    @BeforeMethod(groups = { "simple" }, timeOut = SETUP_TIMEOUT)
    public void beforeTest(Method method) {
        spyClient.clearCapturedRequests();
    }

    private List<String> getAuthTokenInRequests() {
        return spyClient.getCapturedRequests().stream()
                .map(r -> r.getHeaders().get(HttpConstants.HttpHeaders.AUTHORIZATION)).collect(Collectors.toList());
    }

    @Test(groups = { "simple" }, timeOut = TIMEOUT * 100000)
    public void validateLowerCasePercentEncodingOfAuthorizationHeader() {
        spyClient.readCollection(createdCollection.getSelfLink(), null).toBlocking().single();
        spyClient.createDocument(createdCollection.getSelfLink(), new Document(), null, false).toBlocking().single().getResource();

        for (String authHeader : getAuthTokenInRequests()) {
            Matcher percentEncodingMatcher = URL_PERCENT_ENCODING.matcher(authHeader);
            while (percentEncodingMatcher.find()) {
                String matchingSubstring = percentEncodingMatcher.group(); 
                assertThat(matchingSubstring).isEqualTo(matchingSubstring.toLowerCase());
            }
        }
    }
}