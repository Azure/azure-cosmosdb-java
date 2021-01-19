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

package com.microsoft.azure.cosmosdb.rx.internal;

import com.microsoft.azure.cosmosdb.BridgeInternal;
import com.microsoft.azure.cosmosdb.DocumentClientException;
import com.microsoft.azure.cosmosdb.RetryOptions;
import com.microsoft.azure.cosmosdb.internal.HttpConstants;
import com.microsoft.azure.cosmosdb.internal.OperationType;
import com.microsoft.azure.cosmosdb.internal.ResourceType;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.GoneException;
import io.netty.handler.timeout.ReadTimeoutException;
import org.mockito.Mockito;
import org.testng.annotations.Test;
import rx.Completable;
import rx.Single;
import rx.observers.TestSubscriber;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import java.net.URL;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class ClientRetryPolicyTest {
    private final static int TIMEOUT = 10000;

    @Test(groups = "unit")
    public void networkFailureOnRead() throws Exception {
        RetryOptions retryOptions = new RetryOptions();
        GlobalEndpointManager endpointManager = Mockito.mock(GlobalEndpointManager.class);
        Mockito.doReturn(new URL("http://localhost")).when(endpointManager).resolveServiceEndpoint(Mockito.any(RxDocumentServiceRequest.class));
        Mockito.doReturn(Completable.complete()).when(endpointManager).refreshLocationAsync(Mockito.eq(null), Mockito.eq(false));
        ClientRetryPolicy clientRetryPolicy = new ClientRetryPolicy(endpointManager, true, retryOptions);

        Exception exception = ReadTimeoutException.INSTANCE;
        DocumentClientException dce = new DocumentClientException(0,exception);
        BridgeInternal.setSubStatusCode(dce, HttpConstants.SubStatusCodes.GATEWAY_ENDPOINT_UNAVAILABLE);

        RxDocumentServiceRequest dsr = RxDocumentServiceRequest.createFromName(
                OperationType.Read, "/dbs/db/colls/col/docs/docId", ResourceType.Document);
        dsr.requestContext = Mockito.mock(DocumentServiceRequestContext.class);

        clientRetryPolicy.onBeforeSendRequest(dsr);

        for (int i = 0; i < 10; i++) {
            Single<IRetryPolicy.ShouldRetryResult> shouldRetry = clientRetryPolicy.shouldRetry(dce);

            validateSuccess(shouldRetry, ShouldRetryValidator.builder()
                    .nullException()
                    .shouldRetry(true)
                    .backOfTime(Duration.ofMillis(ClientRetryPolicy.RetryIntervalInMS))
                    .build());

            Mockito.verify(endpointManager, Mockito.times(i + 1)).markEndpointUnavailableForRead(Mockito.any());
            Mockito.verify(endpointManager, Mockito.times(0)).markEndpointUnavailableForWrite(Mockito.any());
        }
    }

    @Test(groups = "unit")
    public void tcpNetworkFailureOnRead() throws Exception {
        RetryOptions retryOptions = new RetryOptions();
        GlobalEndpointManager endpointManager = Mockito.mock(GlobalEndpointManager.class);
        Mockito.doReturn(new URL("http://localhost")).when(endpointManager).resolveServiceEndpoint(Mockito.any(RxDocumentServiceRequest.class));
        Mockito.doReturn(Completable.complete()).when(endpointManager).refreshLocationAsync(Mockito.eq(null), Mockito.eq(false));
        Mockito.doReturn(2).when(endpointManager).getPreferredLocationCount();
        ClientRetryPolicy clientRetryPolicy = new ClientRetryPolicy(endpointManager, true, retryOptions);

        Exception exception = ReadTimeoutException.INSTANCE;
        GoneException goneException = new GoneException(exception);
        DocumentClientException dce = new DocumentClientException(HttpConstants.StatusCodes.SERVICE_UNAVAILABLE,
        goneException);

        RxDocumentServiceRequest dsr = RxDocumentServiceRequest.createFromName(
        OperationType.Read, "/dbs/db/colls/col/docs/docId", ResourceType.Document);
        dsr.requestContext = Mockito.mock(DocumentServiceRequestContext.class);

        clientRetryPolicy.onBeforeSendRequest(dsr);

        for (int i = 0; i < 10; i++) {
            Single<IRetryPolicy.ShouldRetryResult> shouldRetry = clientRetryPolicy.shouldRetry(dce);

            if (i < 2) {
                validateSuccess(shouldRetry, ShouldRetryValidator.builder()
                .nullException()
                .shouldRetry(true)
                .backOfTime(Duration.ofMillis(0))
                .build());
            } else {
                validateSuccess(shouldRetry, ShouldRetryValidator.builder()
                .nullException()
                .shouldRetry(false)
                .build());
            }

            Mockito.verify(endpointManager, Mockito.times(0)).markEndpointUnavailableForRead(Mockito.any());
            Mockito.verify(endpointManager, Mockito.times(0)).markEndpointUnavailableForWrite(Mockito.any());
        }
    }

    @Test(groups = "unit")
    public void networkFailureOnWrite() throws Exception {
        RetryOptions retryOptions = new RetryOptions();
        GlobalEndpointManager endpointManager = Mockito.mock(GlobalEndpointManager.class);
        Mockito.doReturn(new URL("http://localhost")).when(endpointManager).resolveServiceEndpoint(Mockito.any(RxDocumentServiceRequest.class));
        Mockito.doReturn(Completable.complete()).when(endpointManager).refreshLocationAsync(Mockito.eq(null), Mockito.eq(false));
        ClientRetryPolicy clientRetryPolicy = new ClientRetryPolicy(endpointManager, true, retryOptions);

        Exception exception = ReadTimeoutException.INSTANCE;
        DocumentClientException dce = new DocumentClientException(0,exception);
        BridgeInternal.setSubStatusCode(dce, HttpConstants.SubStatusCodes.GATEWAY_ENDPOINT_UNAVAILABLE);

        RxDocumentServiceRequest dsr = RxDocumentServiceRequest.createFromName(
                OperationType.Create, "/dbs/db/colls/col/docs/docId", ResourceType.Document);
        dsr.requestContext = Mockito.mock(DocumentServiceRequestContext.class);

        clientRetryPolicy.onBeforeSendRequest(dsr);
        for (int i = 0; i < 10; i++) {
            Single<IRetryPolicy.ShouldRetryResult> shouldRetry = clientRetryPolicy.shouldRetry(dce);
            //  We don't want to retry writes on network failure
            validateSuccess(shouldRetry, ShouldRetryValidator.builder()
                    .nullException()
                    .shouldRetry(false)
                    .build());

            Mockito.verify(endpointManager, Mockito.times(0)).markEndpointUnavailableForRead(Mockito.any());
            Mockito.verify(endpointManager, Mockito.times(i + 1)).markEndpointUnavailableForWrite(Mockito.any());
        }
    }

    @Test(groups = "unit")
    public void tcpNetworkFailureOnWrite() throws Exception {
        RetryOptions retryOptions = new RetryOptions();
        GlobalEndpointManager endpointManager = Mockito.mock(GlobalEndpointManager.class);
        Mockito.doReturn(new URL("http://localhost")).when(endpointManager).resolveServiceEndpoint(Mockito.any(RxDocumentServiceRequest.class));
        Mockito.doReturn(Completable.complete()).when(endpointManager).refreshLocationAsync(Mockito.eq(null), Mockito.eq(false));
        Mockito.doReturn(2).when(endpointManager).getPreferredLocationCount();
        ClientRetryPolicy clientRetryPolicy = new ClientRetryPolicy(endpointManager, true, retryOptions);

        //Non retribale exception for write
        Exception exception = ReadTimeoutException.INSTANCE;
        GoneException goneException = new GoneException(exception);
        DocumentClientException dce = new DocumentClientException(HttpConstants.StatusCodes.SERVICE_UNAVAILABLE,
        goneException);

        RxDocumentServiceRequest dsr = RxDocumentServiceRequest.createFromName(
        OperationType.Create, "/dbs/db/colls/col/docs/docId", ResourceType.Document);
        dsr.requestContext = Mockito.mock(DocumentServiceRequestContext.class);

        clientRetryPolicy.onBeforeSendRequest(dsr);

        for (int i = 0; i < 10; i++) {
            Single<IRetryPolicy.ShouldRetryResult> shouldRetry = clientRetryPolicy.shouldRetry(dce);
            //  We don't want to retry writes on network failure with non retriable exception
            validateSuccess(shouldRetry, ShouldRetryValidator.builder()
            .nullException()
            .shouldRetry(false)
            .build());

            Mockito.verify(endpointManager, Mockito.times(0)).markEndpointUnavailableForRead(Mockito.any());
            Mockito.verify(endpointManager, Mockito.times(0)).markEndpointUnavailableForWrite(Mockito.any());
        }

        // Retriable exception scenario
        exception = new SSLHandshakeException("test");
        goneException = new GoneException(exception);
        dce = new DocumentClientException(HttpConstants.StatusCodes.SERVICE_UNAVAILABLE,
        goneException);

        Mockito.doReturn(true).when(endpointManager).CanUseMultipleWriteLocations(Mockito.any(RxDocumentServiceRequest.class));
        clientRetryPolicy = new ClientRetryPolicy(endpointManager, true, retryOptions);
        clientRetryPolicy.onBeforeSendRequest(dsr);

        for (int i = 0; i < 10; i++) {
            Single<IRetryPolicy.ShouldRetryResult> shouldRetry = clientRetryPolicy.shouldRetry(dce);
            //  We want to retry writes on network failure with retriable exception
            if (i < 2) {
                validateSuccess(shouldRetry, ShouldRetryValidator.builder()
                .nullException()
                .shouldRetry(true)
                .backOfTime(Duration.ofMillis(0))
                .build());
            } else {
                validateSuccess(shouldRetry, ShouldRetryValidator.builder()
                .nullException()
                .shouldRetry(false)
                .build());
            }

            Mockito.verify(endpointManager, Mockito.times(0)).markEndpointUnavailableForRead(Mockito.any());
            Mockito.verify(endpointManager, Mockito.times(0)).markEndpointUnavailableForWrite(Mockito.any());
        }
    }

    @Test(groups = "unit")
    public void networkFailureOnUpsert() throws Exception {
        RetryOptions retryOptions = new RetryOptions();
        GlobalEndpointManager endpointManager = Mockito.mock(GlobalEndpointManager.class);
        Mockito.doReturn(new URL("http://localhost")).when(endpointManager).resolveServiceEndpoint(Mockito.any(RxDocumentServiceRequest.class));
        Mockito.doReturn(Completable.complete()).when(endpointManager).refreshLocationAsync(Mockito.eq(null), Mockito.eq(false));
        ClientRetryPolicy clientRetryPolicy = new ClientRetryPolicy(endpointManager, true, retryOptions);

        Exception exception = ReadTimeoutException.INSTANCE;
        DocumentClientException dce = new DocumentClientException(0,exception);
        BridgeInternal.setSubStatusCode(dce, HttpConstants.SubStatusCodes.GATEWAY_ENDPOINT_UNAVAILABLE);

        RxDocumentServiceRequest dsr = RxDocumentServiceRequest.createFromName(
            OperationType.Upsert, "/dbs/db/colls/col/docs/docId", ResourceType.Document);
        dsr.requestContext = Mockito.mock(DocumentServiceRequestContext.class);

        clientRetryPolicy.onBeforeSendRequest(dsr);
        for (int i = 0; i < 10; i++) {
            Single<IRetryPolicy.ShouldRetryResult> shouldRetry = clientRetryPolicy.shouldRetry(dce);
            //  We don't want to retry writes on network failure
            validateSuccess(shouldRetry, ShouldRetryValidator.builder()
                                                             .nullException()
                                                             .shouldRetry(false)
                                                             .build());

            Mockito.verify(endpointManager, Mockito.times(0)).markEndpointUnavailableForRead(Mockito.any());
            Mockito.verify(endpointManager, Mockito.times(i + 1)).markEndpointUnavailableForWrite(Mockito.any());
        }
    }

    @Test(groups = "unit")
    public void tcpNetworkFailureOnUpsert() throws Exception {
        RetryOptions retryOptions = new RetryOptions();
        GlobalEndpointManager endpointManager = Mockito.mock(GlobalEndpointManager.class);
        Mockito.doReturn(new URL("http://localhost")).when(endpointManager).resolveServiceEndpoint(Mockito.any(RxDocumentServiceRequest.class));
        Mockito.doReturn(Completable.complete()).when(endpointManager).refreshLocationAsync(Mockito.eq(null), Mockito.eq(false));
        Mockito.doReturn(2).when(endpointManager).getPreferredLocationCount();
        ClientRetryPolicy clientRetryPolicy = new ClientRetryPolicy(endpointManager, true, retryOptions);

        Exception exception = ReadTimeoutException.INSTANCE;
        GoneException goneException = new GoneException(exception);
        DocumentClientException dce = new DocumentClientException(HttpConstants.StatusCodes.SERVICE_UNAVAILABLE,
        goneException);

        RxDocumentServiceRequest dsr = RxDocumentServiceRequest.createFromName(
        OperationType.Upsert, "/dbs/db/colls/col/docs/docId", ResourceType.Document);
        dsr.requestContext = Mockito.mock(DocumentServiceRequestContext.class);

        clientRetryPolicy.onBeforeSendRequest(dsr);

        for (int i = 0; i < 10; i++) {
            Single<IRetryPolicy.ShouldRetryResult> shouldRetry = clientRetryPolicy.shouldRetry(dce);
            //  We don't want to retry writes on network failure with non retriable exception
            validateSuccess(shouldRetry, ShouldRetryValidator.builder()
            .nullException()
            .shouldRetry(false)
            .build());

            Mockito.verify(endpointManager, Mockito.times(0)).markEndpointUnavailableForRead(Mockito.any());
            Mockito.verify(endpointManager, Mockito.times(0)).markEndpointUnavailableForWrite(Mockito.any());
        }
    }

    @Test(groups = "unit")
    public void networkFailureOnDelete() throws Exception {
        RetryOptions retryOptions = new RetryOptions();
        GlobalEndpointManager endpointManager = Mockito.mock(GlobalEndpointManager.class);
        Mockito.doReturn(new URL("http://localhost")).when(endpointManager).resolveServiceEndpoint(Mockito.any(RxDocumentServiceRequest.class));
        Mockito.doReturn(Completable.complete()).when(endpointManager).refreshLocationAsync(Mockito.eq(null), Mockito.eq(false));
        ClientRetryPolicy clientRetryPolicy = new ClientRetryPolicy(endpointManager, true, retryOptions);

        Exception exception = ReadTimeoutException.INSTANCE;
        DocumentClientException dce = new DocumentClientException(0,exception);
        BridgeInternal.setSubStatusCode(dce, HttpConstants.SubStatusCodes.GATEWAY_ENDPOINT_UNAVAILABLE);

        RxDocumentServiceRequest dsr = RxDocumentServiceRequest.createFromName(
            OperationType.Delete, "/dbs/db/colls/col/docs/docId", ResourceType.Document);
        dsr.requestContext = Mockito.mock(DocumentServiceRequestContext.class);

        clientRetryPolicy.onBeforeSendRequest(dsr);
        for (int i = 0; i < 10; i++) {
            Single<IRetryPolicy.ShouldRetryResult> shouldRetry = clientRetryPolicy.shouldRetry(dce);
            //  We don't want to retry writes on network failure
            validateSuccess(shouldRetry, ShouldRetryValidator.builder()
                                                             .nullException()
                                                             .shouldRetry(false)
                                                             .build());

            Mockito.verify(endpointManager, Mockito.times(0)).markEndpointUnavailableForRead(Mockito.any());
            Mockito.verify(endpointManager, Mockito.times(i + 1)).markEndpointUnavailableForWrite(Mockito.any());
        }
    }

    @Test(groups = "unit")
    public void tcpNetworkFailureOnDelete() throws Exception {
        RetryOptions retryOptions = new RetryOptions();
        GlobalEndpointManager endpointManager = Mockito.mock(GlobalEndpointManager.class);
        Mockito.doReturn(new URL("http://localhost")).when(endpointManager).resolveServiceEndpoint(Mockito.any(RxDocumentServiceRequest.class));
        Mockito.doReturn(Completable.complete()).when(endpointManager).refreshLocationAsync(Mockito.eq(null), Mockito.eq(false));
        Mockito.doReturn(2).when(endpointManager).getPreferredLocationCount();
        ClientRetryPolicy clientRetryPolicy = new ClientRetryPolicy(endpointManager, true, retryOptions);

        Exception exception = ReadTimeoutException.INSTANCE;
        GoneException goneException = new GoneException(exception);
        DocumentClientException dce = new DocumentClientException(HttpConstants.StatusCodes.SERVICE_UNAVAILABLE,
        goneException);

        RxDocumentServiceRequest dsr = RxDocumentServiceRequest.createFromName(
        OperationType.Delete, "/dbs/db/colls/col/docs/docId", ResourceType.Document);
        dsr.requestContext = Mockito.mock(DocumentServiceRequestContext.class);

        clientRetryPolicy.onBeforeSendRequest(dsr);

        for (int i = 0; i < 10; i++) {
            Single<IRetryPolicy.ShouldRetryResult> shouldRetry = clientRetryPolicy.shouldRetry(dce);
            //  We don't want to retry writes on network failure with non retriable exception
            validateSuccess(shouldRetry, ShouldRetryValidator.builder()
            .nullException()
            .shouldRetry(false)
            .build());

            Mockito.verify(endpointManager, Mockito.times(0)).markEndpointUnavailableForRead(Mockito.any());
            Mockito.verify(endpointManager, Mockito.times(0)).markEndpointUnavailableForWrite(Mockito.any());
        }
    }

    @Test(groups = "unit")
    public void onBeforeSendRequestNotInvoked() {
        RetryOptions retryOptions = new RetryOptions();
        GlobalEndpointManager endpointManager = Mockito.mock(GlobalEndpointManager.class);

        Mockito.doReturn(Completable.complete()).when(endpointManager).refreshLocationAsync(Mockito.eq(null), Mockito.eq(false));
        ClientRetryPolicy clientRetryPolicy = new ClientRetryPolicy(endpointManager, true, retryOptions);

        Exception exception = ReadTimeoutException.INSTANCE;

        RxDocumentServiceRequest dsr = RxDocumentServiceRequest.createFromName(
                OperationType.Create, "/dbs/db/colls/col/docs/docId", ResourceType.Document);
        dsr.requestContext = Mockito.mock(DocumentServiceRequestContext.class);

        Single<IRetryPolicy.ShouldRetryResult> shouldRetry = clientRetryPolicy.shouldRetry(exception);
        validateSuccess(shouldRetry, ShouldRetryValidator.builder()
                .withException(exception)
                .shouldRetry(false)
                .build());

        Mockito.verifyZeroInteractions(endpointManager);
    }

    public static void validateSuccess(Single<IRetryPolicy.ShouldRetryResult> single,
                                       ShouldRetryValidator validator) {

        validateSuccess(single, validator, TIMEOUT);
    }

    public static void validateSuccess(Single<IRetryPolicy.ShouldRetryResult> single,
                                       ShouldRetryValidator validator,
                                       long timeout) {
        TestSubscriber<IRetryPolicy.ShouldRetryResult> testSubscriber = new TestSubscriber<>();

        single.toObservable().subscribe(testSubscriber);
        testSubscriber.awaitTerminalEvent(timeout, TimeUnit.MILLISECONDS);
        testSubscriber.assertCompleted();
        testSubscriber.assertNoErrors();
        testSubscriber.assertValueCount(1);
        validator.validate(testSubscriber.getOnNextEvents().get(0));
    }
}
