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
import com.microsoft.azure.cosmosdb.ConsistencyLevel;
import com.microsoft.azure.cosmosdb.DocumentClientException;
import com.microsoft.azure.cosmosdb.Error;
import com.microsoft.azure.cosmosdb.ISessionContainer;
import com.microsoft.azure.cosmosdb.internal.HttpConstants;
import com.microsoft.azure.cosmosdb.internal.OperationType;
import com.microsoft.azure.cosmosdb.internal.PathsHelper;
import com.microsoft.azure.cosmosdb.internal.QueryCompatibilityMode;
import com.microsoft.azure.cosmosdb.internal.ResourceType;
import com.microsoft.azure.cosmosdb.internal.RuntimeConstants;
import com.microsoft.azure.cosmosdb.internal.UserAgentContainer;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.HttpUtils;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.StoreResponse;
import com.microsoft.azure.cosmosdb.rx.internal.http.HttpHeaders;
import com.microsoft.azure.cosmosdb.rx.internal.http.HttpRequest;
import com.microsoft.azure.cosmosdb.rx.internal.http.HttpResponse;
import hu.akarnokd.rxjava.interop.RxJavaInterop;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import rx.Observable;
import rx.Single;
import rx.functions.Func0;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * While this class is public, but it is not part of our published public APIs.
 * This is meant to be internally used only by our sdk.
 *
 * Used internally to provide functionality to communicate and process response from Gateway in the Azure Cosmos DB database service.
 */
class RxGatewayStoreModel implements RxStoreModel {

    private final static int INITIAL_RESPONSE_BUFFER_SIZE = 1024;
    private final Logger logger = LoggerFactory.getLogger(RxGatewayStoreModel.class);
    private final Map<String, String> defaultHeaders;
    private final com.microsoft.azure.cosmosdb.rx.internal.http.HttpClient httpClient;
    private final QueryCompatibilityMode queryCompatibilityMode;
    private final GlobalEndpointManager globalEndpointManager;
    private ConsistencyLevel defaultConsistencyLevel;
    private ISessionContainer sessionContainer;

    public RxGatewayStoreModel(
            ISessionContainer sessionContainer,
            ConsistencyLevel defaultConsistencyLevel,
            QueryCompatibilityMode queryCompatibilityMode,
            UserAgentContainer userAgentContainer,
            GlobalEndpointManager globalEndpointManager,
            com.microsoft.azure.cosmosdb.rx.internal.http.HttpClient httpClient) {
        this.defaultHeaders = new HashMap<>();
        this.defaultHeaders.put(HttpConstants.HttpHeaders.CACHE_CONTROL,
                "no-cache");
        this.defaultHeaders.put(HttpConstants.HttpHeaders.VERSION,
                HttpConstants.Versions.CURRENT_VERSION);

        if (userAgentContainer == null) {
            userAgentContainer = new UserAgentContainer();
        }

        this.defaultHeaders.put(HttpConstants.HttpHeaders.USER_AGENT, userAgentContainer.getUserAgent());

        if (defaultConsistencyLevel != null) {
            this.defaultHeaders.put(HttpConstants.HttpHeaders.CONSISTENCY_LEVEL,
                    defaultConsistencyLevel.toString());
        }

        this.defaultConsistencyLevel = defaultConsistencyLevel;
        this.globalEndpointManager = globalEndpointManager;
        this.queryCompatibilityMode = queryCompatibilityMode;

        this.httpClient = httpClient;
        this.sessionContainer = sessionContainer;
    }

    private Observable<RxDocumentServiceResponse> doCreate(RxDocumentServiceRequest request) {
        return this.performRequest(request, HttpMethod.POST);
    }

    private Observable<RxDocumentServiceResponse> upsert(RxDocumentServiceRequest request) {
        return this.performRequest(request, HttpMethod.POST);
    }

    private Observable<RxDocumentServiceResponse> read(RxDocumentServiceRequest request) {
        return this.performRequest(request, HttpMethod.GET);
    }

    private Observable<RxDocumentServiceResponse> replace(RxDocumentServiceRequest request) {
        return this.performRequest(request, HttpMethod.PUT);
    }

    private Observable<RxDocumentServiceResponse> delete(RxDocumentServiceRequest request) {
        return this.performRequest(request, HttpMethod.DELETE);
    }

    private Observable<RxDocumentServiceResponse> execute(RxDocumentServiceRequest request) {
        return this.performRequest(request, HttpMethod.POST);
    }

    private Observable<RxDocumentServiceResponse> readFeed(RxDocumentServiceRequest request) {
        return this.performRequest(request, HttpMethod.GET);
    }

    private Observable<RxDocumentServiceResponse> query(RxDocumentServiceRequest request) {
        request.getHeaders().put(HttpConstants.HttpHeaders.IS_QUERY, "true");

        switch (this.queryCompatibilityMode) {
            case SqlQuery:
                request.getHeaders().put(HttpConstants.HttpHeaders.CONTENT_TYPE,
                        RuntimeConstants.MediaTypes.SQL);
                break;
            case Default:
            case Query:
            default:
                request.getHeaders().put(HttpConstants.HttpHeaders.CONTENT_TYPE,
                        RuntimeConstants.MediaTypes.QUERY_JSON);
                break;
        }
        return this.performRequest(request, HttpMethod.POST);
    }

    /**
     * Given the request it creates an observable which upon subscription issues HTTP call and emits one RxDocumentServiceResponse.
     *
     * @param request
     * @param method
     * @return Observable<RxDocumentServiceResponse>
     */
    public Observable<RxDocumentServiceResponse> performRequest(RxDocumentServiceRequest request, HttpMethod method) {

        try {
            URI uri = getUri(request);

            com.microsoft.azure.cosmosdb.rx.internal.http.HttpHeaders httpHeaders = this.getHttpRequestHeaders(request.getHeaders());

            Flux<ByteBuf> byteBufObservable = Flux.empty();

            if (request.getContentObservable() != null) {
                Observable<ByteBuf> bufObservable = request.getContentObservable().map(Unpooled::wrappedBuffer);
                byteBufObservable = Flux.from(RxJavaInterop.toV2Flowable(bufObservable));
            } else if (request.getContent() != null){
                byteBufObservable = Flux.just(Unpooled.wrappedBuffer(request.getContent()));
            }


            HttpRequest httpRequest = new HttpRequest(com.microsoft.azure.cosmosdb.rx.internal.http.HttpMethod.valueOf(method.name()),
                    uri.toURL(),
                    httpHeaders,
                    byteBufObservable);

            Mono<HttpResponse> httpResponseMono = this.httpClient
                    .port(uri.getPort())
                    .send(httpRequest);

            return toDocumentServiceResponse(httpResponseMono, request);

        } catch (Exception e) {
            return Observable.error(e);
        }
    }

    private com.microsoft.azure.cosmosdb.rx.internal.http.HttpHeaders getHttpRequestHeaders(Map<String, String> headers) {
        com.microsoft.azure.cosmosdb.rx.internal.http.HttpHeaders httpHeaders = new com.microsoft.azure.cosmosdb.rx.internal.http.HttpHeaders();
        // Add default headers.
        for (Entry<String, String> entry : this.defaultHeaders.entrySet()) {
            if (!headers.containsKey(entry.getKey())) {
                // populate default header only if there is no overwrite by the request header
                httpHeaders.set(entry.getKey(), entry.getValue());
            }
        }

        // Add override headers.
        if (headers != null) {
            for (Entry<String, String> entry : headers.entrySet()) {
                if (entry.getValue() == null) {
                    // netty doesn't allow setting null value in header
                    httpHeaders.set(entry.getKey(), "");
                } else {
                    httpHeaders.set(entry.getKey(), entry.getValue());
                }
            }
        }
        return httpHeaders;
    }

    private URI getUri(RxDocumentServiceRequest request) throws URISyntaxException {
        URI rootUri = request.getEndpointOverride();
        if (rootUri == null) {
            if (request.getIsMedia()) {
                // For media read request, always use the write endpoint.
                rootUri = this.globalEndpointManager.getWriteEndpoints().get(0).toURI();
            } else {
                rootUri = this.globalEndpointManager.resolveServiceEndpoint(request).toURI();
            }
        }

        String path = PathsHelper.generatePath(request.getResourceType(), request, request.isFeed);
        if(request.getResourceType().equals(ResourceType.DatabaseAccount)) {
            path = StringUtils.EMPTY;
        }

        URI uri = new URI("https",
                null,
                rootUri.getHost(),
                rootUri.getPort(),
                ensureSlashPrefixed(path),
                null,  // Query string not used.
                null);

        return uri;
    }

    private String ensureSlashPrefixed(String path) {
        if (path == null) {
            return path;
        }

        if (path.startsWith("/")) {
            return path;
        }

        return "/" + path;
    }

    /**
     * Transforms the reactor netty's client response Observable to RxDocumentServiceResponse Observable.
     *
     *
     * Once the customer code subscribes to the observable returned by the CRUD APIs,
     * the subscription goes up till it reaches the source reactor netty's observable, and at that point the HTTP invocation will be made.
     *
     * @param httpResponseMono
     * @param request
     * @return {@link Observable}
     */
    private Observable<RxDocumentServiceResponse> toDocumentServiceResponse(Mono<HttpResponse> httpResponseMono,
                                                                            RxDocumentServiceRequest request) {

        if (request.getIsMedia()) {
            return RxJavaInterop.toV1Observable(httpResponseMono.flatMap(httpResponse -> {

                // header key/value pairs
                com.microsoft.azure.cosmosdb.rx.internal.http.HttpHeaders httpResponseHeaders = httpResponse.headers();
                int httpResponseStatus = httpResponse.statusCode();

                Flux<InputStream> inputStreamObservable;

                if (request.getOperationType() == OperationType.Delete) {
                    // for delete we don't expect any body
                    inputStreamObservable = Flux.just(IOUtils.toInputStream("", StandardCharsets.UTF_8));
                } else {
                    // transforms the ByteBufFlux to Flux<InputStream>
                    inputStreamObservable = httpResponse
                            .body()
                            .flatMap(byteBuf ->
                                    Flux.just(IOUtils.toInputStream(byteBuf.toString(StandardCharsets.UTF_8), StandardCharsets.UTF_8)));
                }

                return inputStreamObservable
                        .flatMap(contentInputStream -> {
                            try {
                                // If there is any error in the header response this throws exception
                                // TODO: potential performance improvement: return Observable.error(exception) on failure instead of throwing Exception
                                validateOrThrow(request,
                                        HttpResponseStatus.valueOf(httpResponseStatus),
                                        httpResponseHeaders,
                                        null,
                                        contentInputStream);

                                // transforms to Observable<StoreResponse>
                                StoreResponse rsp = new StoreResponse(httpResponseStatus, HttpUtils
                                        .unescape(new ArrayList<>(httpResponseHeaders.toMap().entrySet())), contentInputStream);
                                return Flux.just(rsp);
                            } catch (Exception e) {
                                return Flux.error(reactor.core.Exceptions.propagate(e));
                            }
                        }).single();

            }).map(RxDocumentServiceResponse::new));

        } else {
            return RxJavaInterop.toV1Observable(httpResponseMono.flatMap(httpResponse ->  {

                // header key/value pairs
                HttpHeaders httpResponseHeaders = httpResponse.headers();
                int httpResponseStatus = httpResponse.statusCode();

                Flux<String> contentObservable;

                if (request.getOperationType() == OperationType.Delete) {
                    // for delete we don't expect any body
                    contentObservable = Flux.just("");
                } else {
                    // transforms the ByteBufFlux to Flux<String>
                    contentObservable = toString(httpResponse.body());
                }

                return contentObservable
                        .flatMap(content -> {
                            try {
                                // If there is any error in the header response this throws exception
                                // TODO: potential performance improvement: return Observable.error(exception) on failure instead of throwing Exception
                                validateOrThrow(request, HttpResponseStatus.valueOf(httpResponseStatus), httpResponseHeaders, content, null);

                                // transforms to Observable<StoreResponse>
                                StoreResponse rsp = new StoreResponse(httpResponseStatus, HttpUtils.unescape(
                                        new ArrayList<>(httpResponseHeaders.toMap().entrySet())), content);
                                return Flux.just(rsp);
                            } catch (Exception e) {
                                return Flux.error(reactor.core.Exceptions.propagate(e));
                            }
                        }).single();

            }).map(RxDocumentServiceResponse::new)
                    .onErrorResume(throwable -> {
                        if (!(throwable instanceof Exception)) {
                            // fatal error
                            logger.error("Unexpected failure {}", throwable.getMessage(), throwable);
                            return Mono.error(reactor.core.Exceptions.propagate(throwable));
                        }

                        Exception exception = (Exception) throwable;
                        if (!(exception instanceof DocumentClientException)) {
                            // wrap in DocumentClientException
                            logger.error("Network failure", exception);
                            DocumentClientException dce = new DocumentClientException(0, exception);
                            BridgeInternal.setRequestHeaders(dce, request.getHeaders());
                            return Mono.error(reactor.core.Exceptions.propagate(dce));
                        }

                        return Mono.error(reactor.core.Exceptions.propagate(exception));
                    }));
        }
    }

    private void validateOrThrow(RxDocumentServiceRequest request, HttpResponseStatus status, com.microsoft.azure.cosmosdb.rx.internal.http.HttpHeaders headers, String body,
                                 InputStream inputStream) throws DocumentClientException {

        int statusCode = status.code();

        if (statusCode >= HttpConstants.StatusCodes.MINIMUM_STATUSCODE_AS_ERROR_GATEWAY) {
            if (body == null && inputStream != null) {
                try {
                    body = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
                } catch (IOException e) {
                    logger.error("Failed to get content from the http response", e);
                    DocumentClientException dce = new DocumentClientException(0, e);
                    BridgeInternal.setRequestHeaders(dce, request.getHeaders());
                    throw dce;
                } finally {
                    IOUtils.closeQuietly(inputStream);
                }
            }

            String statusCodeString = status.reasonPhrase() != null
                    ? status.reasonPhrase().replace(" ", "")
                    : "";
            Error error = null;
            error = (body != null) ? new Error(body) : new Error();
            error = new Error(statusCodeString,
                    String.format("%s, StatusCode: %s", error.getMessage(), statusCodeString),
                    error.getPartitionedQueryExecutionInfo());

            DocumentClientException dce = new DocumentClientException(statusCode, error, headers.toMap());
            BridgeInternal.setRequestHeaders(dce, request.getHeaders());
            throw dce;
        }
    }

    private Observable<RxDocumentServiceResponse> invokeAsyncInternal(RxDocumentServiceRequest request)  {
        switch (request.getOperationType()) {
            case Create:
                return this.doCreate(request);
            case Upsert:
                return this.upsert(request);
            case Delete:
                return this.delete(request);
            case ExecuteJavaScript:
                return this.execute(request);
            case Read:
                return this.read(request);
            case ReadFeed:
                return this.readFeed(request);
            case Replace:
                return this.replace(request);
            case SqlQuery:
            case Query:
                return this.query(request);
            default:
                throw new IllegalStateException("Unknown operation type " + request.getOperationType());
        }
    }

    private Observable<RxDocumentServiceResponse> invokeAsync(RxDocumentServiceRequest request) {
        Func0<Single<RxDocumentServiceResponse>> funcDelegate = () -> invokeAsyncInternal(request).toSingle();
        return BackoffRetryUtility.executeRetry(funcDelegate, new WebExceptionRetryPolicy()).toObservable();
    }

    @Override
    public Observable<RxDocumentServiceResponse> processMessage(RxDocumentServiceRequest request) {
        this.applySessionToken(request);

        Observable<RxDocumentServiceResponse> responseObs = invokeAsync(request);

        return responseObs.onErrorResumeNext(
                e -> {
                    DocumentClientException dce = Utils.as(e, DocumentClientException.class);

                    if (dce == null) {
                        logger.error("unexpected failure {}", e.getMessage(), e);
                        return Observable.error(e);
                    }

                    if ((!ReplicatedResourceClientUtils.isMasterResource(request.getResourceType())) &&
                            (dce.getStatusCode() == HttpConstants.StatusCodes.PRECONDITION_FAILED ||
                                    dce.getStatusCode() == HttpConstants.StatusCodes.CONFLICT ||
                                    (
                                            dce.getStatusCode() == HttpConstants.StatusCodes.NOTFOUND &&
                                                    !Exceptions.isSubStatusCode(dce,
                                                            HttpConstants.SubStatusCodes.READ_SESSION_NOT_AVAILABLE)))) {
                        this.captureSessionToken(request, dce.getResponseHeaders());
                    }

                    return Observable.error(dce);
                }
        ).map(response ->
                {
                    this.captureSessionToken(request, response.getResponseHeaders());
                    return response;
                }
        );
    }

    private void captureSessionToken(RxDocumentServiceRequest request, Map<String, String> responseHeaders) {
        if (request.getResourceType() == ResourceType.DocumentCollection && request.getOperationType() == OperationType.Delete) {
            String resourceId;
            if (request.getIsNameBased()) {
                resourceId = responseHeaders.get(HttpConstants.HttpHeaders.OWNER_ID);
            } else {
                resourceId = request.getResourceId();
            }
            this.sessionContainer.clearTokenByResourceId(resourceId);
        } else {
            this.sessionContainer.setSessionToken(request, responseHeaders);
        }
    }

    private void applySessionToken(RxDocumentServiceRequest request) {
        Map<String, String> headers = request.getHeaders();

        if (headers != null &&
                !Strings.isNullOrEmpty(request.getHeaders().get(HttpConstants.HttpHeaders.SESSION_TOKEN))) {
            if (ReplicatedResourceClientUtils.isMasterResource(request.getResourceType())) {
                request.getHeaders().remove(HttpConstants.HttpHeaders.SESSION_TOKEN);
            }
            return; //User is explicitly controlling the session.
        }

        String requestConsistencyLevel = headers.get(HttpConstants.HttpHeaders.CONSISTENCY_LEVEL);

        boolean sessionConsistency =
                this.defaultConsistencyLevel == ConsistencyLevel.Session ||
                        (!Strings.isNullOrEmpty(requestConsistencyLevel)
                                && Strings.areEqual(requestConsistencyLevel, ConsistencyLevel.Session.name()));

        if (!sessionConsistency || ReplicatedResourceClientUtils.isMasterResource(request.getResourceType())) {
            return; // Only apply the session token in case of session consistency and when resource is not a master resource
        }

        //Apply the ambient session.
        String sessionToken = this.sessionContainer.resolveGlobalSessionToken(request);

        if (!Strings.isNullOrEmpty(sessionToken)) {
            headers.put(HttpConstants.HttpHeaders.SESSION_TOKEN, sessionToken);
        }
    }

    private static Flux<String> toString(Flux<ByteBuf> contentObservable) {
        return contentObservable
                .reduce(
                        new ByteArrayOutputStream(INITIAL_RESPONSE_BUFFER_SIZE),
                        (out, bb) -> {
                            try {
                                bb.readBytes(out, bb.readableBytes());
                                return out;
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        })
                .map(out -> {
                    return new String(out.toByteArray(), StandardCharsets.UTF_8);
                }).flux();
    }
}