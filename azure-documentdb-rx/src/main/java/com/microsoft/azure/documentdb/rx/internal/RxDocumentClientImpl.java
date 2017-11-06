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

import static com.microsoft.azure.documentdb.BridgeInternal.documentFromObject;
import static com.microsoft.azure.documentdb.BridgeInternal.toDatabaseAccount;
import static com.microsoft.azure.documentdb.BridgeInternal.toFeedResponsePage;
import static com.microsoft.azure.documentdb.BridgeInternal.toResourceResponse;
import static com.microsoft.azure.documentdb.BridgeInternal.toStoredProcedureResponse;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.documentdb.Attachment;
import com.microsoft.azure.documentdb.BridgeInternal;
import com.microsoft.azure.documentdb.ChangeFeedOptions;
import com.microsoft.azure.documentdb.Conflict;
import com.microsoft.azure.documentdb.ConnectionMode;
import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.DatabaseAccount;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.FeedOptions;
import com.microsoft.azure.documentdb.FeedOptionsBase;
import com.microsoft.azure.documentdb.FeedResponsePage;
import com.microsoft.azure.documentdb.JsonSerializable;
import com.microsoft.azure.documentdb.MediaOptions;
import com.microsoft.azure.documentdb.MediaResponse;
import com.microsoft.azure.documentdb.Offer;
import com.microsoft.azure.documentdb.PartitionKeyRange;
import com.microsoft.azure.documentdb.Permission;
import com.microsoft.azure.documentdb.RequestOptions;
import com.microsoft.azure.documentdb.Resource;
import com.microsoft.azure.documentdb.ResourceResponse;
import com.microsoft.azure.documentdb.SqlQuerySpec;
import com.microsoft.azure.documentdb.StoredProcedure;
import com.microsoft.azure.documentdb.StoredProcedureResponse;
import com.microsoft.azure.documentdb.Trigger;
import com.microsoft.azure.documentdb.User;
import com.microsoft.azure.documentdb.UserDefinedFunction;
import com.microsoft.azure.documentdb.internal.BaseAuthorizationTokenProvider;
import com.microsoft.azure.documentdb.internal.DocumentServiceResponse;
import com.microsoft.azure.documentdb.internal.EndpointManager;
import com.microsoft.azure.documentdb.internal.HttpConstants;
import com.microsoft.azure.documentdb.internal.OperationType;
import com.microsoft.azure.documentdb.internal.Paths;
import com.microsoft.azure.documentdb.internal.QueryCompatibilityMode;
import com.microsoft.azure.documentdb.internal.ResourceType;
import com.microsoft.azure.documentdb.internal.RuntimeConstants;
import com.microsoft.azure.documentdb.internal.SessionContainer;
import com.microsoft.azure.documentdb.internal.UserAgentContainer;
import com.microsoft.azure.documentdb.internal.Utils;
import com.microsoft.azure.documentdb.internal.routing.ClientCollectionCache;
import com.microsoft.azure.documentdb.rx.AsyncDocumentClient;

import io.netty.buffer.ByteBuf;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.channel.RxEventLoopProvider;
import io.reactivex.netty.channel.SingleNioLoopProvider;
import io.reactivex.netty.client.RxClient.ClientConfig;
import io.reactivex.netty.pipeline.ssl.DefaultFactories;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.client.HttpClientBuilder;
import rx.Observable;
import rx.Scheduler;
import rx.functions.Func1;
import rx.functions.Func3;
import rx.functions.Func4;
import rx.internal.util.RxThreadFactory;
import rx.schedulers.Schedulers;
import rx.subjects.BehaviorSubject;
import rx.subjects.PublishSubject;
import rx.subjects.ReplaySubject;

public class RxDocumentClientImpl implements AsyncDocumentClient {

    private final static int MAX_COLLECTION_CACHE_CONCURRENCY = 10;
    private final Logger logger = LoggerFactory.getLogger(RxDocumentClientImpl.class);
    private final String masterKey;
    private final ExecutorService collectionCacheExecutorService;
    private final URI serviceEndpoint;
    private final ConnectionPolicy connectionPolicy;
    private final SessionContainer sessionContainer;
    private final ConsistencyLevel consistencyLevel;
    private final BaseAuthorizationTokenProvider authorizationTokenProvider;
    private final ClientCollectionCache collectionCache;
    private final RxGatewayStoreModel gatewayProxy;
    private final RxWrapperDocumentClientImpl rxWrapperClient;
    private final Scheduler computationScheduler;
    private Map<String, String> resourceTokens;
    /**
     * Compatibility mode: Allows to specify compatibility mode used by client when
     * making query requests. Should be removed when application/sql is no longer
     * supported.
     */
    private final QueryCompatibilityMode queryCompatibilityMode = QueryCompatibilityMode.Default;
    private final HttpClient<ByteBuf, ByteBuf> rxClient;
    private final EndpointManager globalEndpointManager;
    private final ExecutorService computationExecutor;
    private static final ObjectMapper mapper = new ObjectMapper();

    public RxDocumentClientImpl(URI serviceEndpoint, String masterKey, ConnectionPolicy connectionPolicy,
            ConsistencyLevel consistencyLevel, int eventLoopSize, int computationPoolSize) {

        logger.info(
                "Initializing DocumentClient with"
                        + " serviceEndpoint [{}], ConnectionPolicy [{}], ConsistencyLevel [{}]",
                serviceEndpoint, connectionPolicy, consistencyLevel);

        this.masterKey = masterKey;
        this.serviceEndpoint = serviceEndpoint;

        if (connectionPolicy != null) {
            this.connectionPolicy = connectionPolicy;
        } else {
            this.connectionPolicy = new ConnectionPolicy();
        }

        this.sessionContainer = new SessionContainer(this.serviceEndpoint.getHost());
        this.consistencyLevel = consistencyLevel;

        UserAgentContainer userAgentContainer = new UserAgentContainer(Constants.Versions.SDK_NAME,
                Constants.Versions.SDK_VERSION);
        String userAgentSuffix = this.connectionPolicy.getUserAgentSuffix();
        if (userAgentSuffix != null && userAgentSuffix.length() > 0) {
            userAgentContainer.setSuffix(userAgentSuffix);
        }

        if (eventLoopSize <= 0) {
            int cpuCount = Runtime.getRuntime().availableProcessors();
            if (cpuCount >= 4) {
                // do authentication token generation on a scheduler
                computationPoolSize = (cpuCount / 4);
                eventLoopSize = cpuCount - computationPoolSize;
            } else {
                // do authentication token generation on subscription thread
                computationPoolSize = 0;
                eventLoopSize = cpuCount;
            }
            logger.debug(
                    "Auto configuring eventLoop size and computation pool size. CPU cores {[]}, eventLoopSize [{}], computationPoolSize [{}]",
                    cpuCount, eventLoopSize, computationPoolSize);
        }

        logger.debug("EventLoop size [{}]", eventLoopSize);

        synchronized (RxDocumentClientImpl.class) {
            SingleNioLoopProvider rxEventLoopProvider = new SingleNioLoopProvider(1, eventLoopSize);
            RxEventLoopProvider oldEventLoopProvider = RxNetty.useEventLoopProvider(rxEventLoopProvider);
            this.rxClient = httpClientBuilder().build();
            RxNetty.useEventLoopProvider(oldEventLoopProvider);
        }

        if (computationPoolSize > 0) {
            logger.debug("Intensive computation configured on a computation scheduler backed by thread pool size [{}]",
                    computationPoolSize);
            this.computationExecutor = new ThreadPoolExecutor(computationPoolSize, computationPoolSize, 0L,
                    TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(2),
                    new RxThreadFactory("rxdocdb-computation"), new CallerRunsPolicy());

            this.computationScheduler = Schedulers.from(this.computationExecutor);
        } else {
            logger.debug("Intensive computation configured on the subscription thread");
            this.computationExecutor = null;
            this.computationScheduler = Schedulers.immediate();
        }

        this.authorizationTokenProvider = new BaseAuthorizationTokenProvider(this.masterKey);
        this.collectionCacheExecutorService = new ThreadPoolExecutor(1, MAX_COLLECTION_CACHE_CONCURRENCY, 10,
                TimeUnit.MINUTES, new ArrayBlockingQueue<Runnable>(MAX_COLLECTION_CACHE_CONCURRENCY, true),
                new ThreadPoolExecutor.CallerRunsPolicy());

        this.collectionCache = BridgeInternal.createClientCollectionCache(this, collectionCacheExecutorService);

        this.globalEndpointManager = BridgeInternal.createGlobalEndpointManager(this);

        this.gatewayProxy = new RxGatewayStoreModel(this.connectionPolicy, consistencyLevel,
                this.queryCompatibilityMode, this.masterKey, this.resourceTokens, userAgentContainer,
                this.globalEndpointManager, this.rxClient);

        this.rxWrapperClient = new RxWrapperDocumentClientImpl(
                new DocumentClient(serviceEndpoint.toString(), masterKey, connectionPolicy, consistencyLevel));

        // If DirectHttps mode is configured in AsyncDocumentClient.Builder we fallback
        // to RxWrapperDocumentClientImpl. So we should never get here

        if (this.connectionPolicy.getConnectionMode() == ConnectionMode.DirectHttps) {
            throw new UnsupportedOperationException("Direct Https is not supported");
        }
    }

    private HttpClientBuilder<ByteBuf, ByteBuf> httpClientBuilder() {
        HttpClientBuilder<ByteBuf, ByteBuf> builder = RxNetty
                .<ByteBuf, ByteBuf>newHttpClientBuilder(this.serviceEndpoint.getHost(), this.serviceEndpoint.getPort())
                .withSslEngineFactory(DefaultFactories.trustAll()).withMaxConnections(connectionPolicy.getMaxPoolSize())
                .withIdleConnectionsTimeoutMillis(this.connectionPolicy.getIdleConnectionTimeout() * 1000);

        ClientConfig config = new ClientConfig.Builder()
                .readTimeout(connectionPolicy.getRequestTimeout(), TimeUnit.SECONDS).build();
        return builder.config(config);
    }

    @Override
    public URI getServiceEndpoint() {
        return this.serviceEndpoint;
    }

    @Override
    public URI getWriteEndpoint() {
        return this.globalEndpointManager.getWriteEndpoint();
    }

    @Override
    public URI getReadEndpoint() {
        return this.globalEndpointManager.getReadEndpoint();
    }

    @Override
    public ConnectionPolicy getConnectionPolicy() {
        return this.connectionPolicy;
    }

    @Override
    public Observable<ResourceResponse<Database>> createDatabase(Database database, RequestOptions options) {

        return Observable.defer(() -> {
            try {

                if (database == null) {
                    throw new IllegalArgumentException("Database");
                }

                logger.debug("Creating a Database. id: [{}]", database.getId());
                validateResource(database);

                Map<String, String> requestHeaders = this.getRequestHeaders(options);
                RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.Create,
                        ResourceType.Database, Paths.DATABASES_ROOT, database, requestHeaders);

                return this.doCreate(request).map(response -> toResourceResponse(response, Database.class));
            } catch (Exception e) {
                logger.debug("Failure in creating a database. due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });
    }

    @Override
    public Observable<ResourceResponse<Database>> deleteDatabase(String databaseLink, RequestOptions options) {
        return Observable.defer(() -> {
            try {
                if (StringUtils.isEmpty(databaseLink)) {
                    throw new IllegalArgumentException("databaseLink");
                }

                logger.debug("Deleting a Database. databaseLink: [{}]", databaseLink);
                String path = Utils.joinPath(databaseLink, null);
                Map<String, String> requestHeaders = this.getRequestHeaders(options);
                RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.Delete,
                        ResourceType.Database, path, requestHeaders);

                return this.doDelete(request).map(response -> toResourceResponse(response, Database.class));
            } catch (Exception e) {
                logger.debug("Failure in deleting a database. due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });
    }

    @Override
    public Observable<ResourceResponse<Database>> readDatabase(String databaseLink, RequestOptions options) {

        return Observable.defer(() -> {
            try {
                if (StringUtils.isEmpty(databaseLink)) {
                    throw new IllegalArgumentException("databaseLink");
                }

                logger.debug("Reading a Database. databaseLink: [{}]", databaseLink);
                String path = Utils.joinPath(databaseLink, null);
                Map<String, String> requestHeaders = this.getRequestHeaders(options);
                RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.Read,
                        ResourceType.Database, path, requestHeaders);

                return this.doRead(request).map(response -> toResourceResponse(response, Database.class));
            } catch (Exception e) {
                logger.debug("Failure in reading a database. due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });
    }

    @Override
    public Observable<FeedResponsePage<Database>> readDatabases(FeedOptions options) {
        
        if (options == null)
            options = new FeedOptions();
        
        return getObservableFeedResponsePage(null, options, Database.class,
                new Func4<String, String, Map<String, String>, RxDocumentClientImpl, Observable<FeedResponsePage<Database>>>() {

                    @Override
                    public Observable<FeedResponsePage<Database>> call(String token, String resourceLink,
                            Map<String, String> requestHeaders, RxDocumentClientImpl client) {
                        if (token != null)
                            requestHeaders.put(HttpConstants.HttpHeaders.CONTINUATION, token);

                        RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.ReadFeed,
                                ResourceType.Database, Paths.DATABASES_ROOT, requestHeaders);
                        try {
                            return client.doReadFeed(request).map(response -> toFeedResponsePage(response, Database.class));
                        } catch (DocumentClientException e) {
                            return Observable.error(e);
                        }
                    }
                });
    }

    private <T extends Resource> Observable<FeedResponsePage<T>> getObservableFeedResponsePage(String resourceLink,
            FeedOptions options, Class<T> cls,
            Func4<String, String, Map<String, String>, RxDocumentClientImpl, Observable<FeedResponsePage<T>>> getObservable) {

        return Observable.defer(() -> {
            try {
                
                logger.debug("Reading " + cls.getClass() + "s");
                Map<String, String> requestHeaders = getFeedHeaders(options);
                return getObservable.call(options.getRequestContinuation(), resourceLink, requestHeaders, this).single()
                        .concatMap(firstPage -> {
                            
                            BehaviorSubject<FeedResponsePage<T>> pagingSubject = BehaviorSubject.<FeedResponsePage<T>>create();
                            Observable<FeedResponsePage<T>> feedResponsePageObservable = pagingSubject
                                    .asObservable()
                                    .concatMap(previousPage -> {
                                        String token = previousPage.getResponseContinuation();
                                        if (token != null) {
                                            return getObservable.call(token, resourceLink, requestHeaders, this)
                                                    .doOnNext(page -> {
                                                        pagingSubject.onNext(page);
                                                    });
                                        } else {
                                            return Observable.<FeedResponsePage<T>>empty()
                                                    .doOnCompleted(() -> pagingSubject.onCompleted());
                                        }
                                    });

                            pagingSubject.onNext(firstPage);
                             
                            return Observable.just(firstPage).concatWith(feedResponsePageObservable);
                        });
            } catch (Exception e) {
                logger.debug("Failure in reading " + cls.getClass() + "s due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });
    }
    
    @Override
    public Observable<FeedResponsePage<Database>> queryDatabases(String query, FeedOptions options) {
        return this.rxWrapperClient.queryDatabases(query, options);
    }

    @Override
    public Observable<FeedResponsePage<Database>> queryDatabases(SqlQuerySpec querySpec, FeedOptions options) {
        return this.rxWrapperClient.queryDatabases(querySpec, options);
    }

    @Override
    public Observable<ResourceResponse<DocumentCollection>> createCollection(String databaseLink,
            DocumentCollection collection, RequestOptions options) {

        return Observable.defer(() -> {
            try {
                if (StringUtils.isEmpty(databaseLink)) {
                    throw new IllegalArgumentException("databaseLink");
                }
                if (collection == null) {
                    throw new IllegalArgumentException("collection");
                }

                logger.debug("Creating a Collection. databaseLink: [{}], Collection id: [{}]", databaseLink,
                        collection.getId());
                validateResource(collection);

                String path = Utils.joinPath(databaseLink, Paths.COLLECTIONS_PATH_SEGMENT);
                Map<String, String> requestHeaders = this.getRequestHeaders(options);
                RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.Create,
                        ResourceType.DocumentCollection, path, collection, requestHeaders);
                return this.doCreate(request).map(response -> toResourceResponse(response, DocumentCollection.class));
            } catch (Exception e) {
                logger.debug("Failure in creating a collection. due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });
    }

    @Override
    public Observable<ResourceResponse<DocumentCollection>> replaceCollection(DocumentCollection collection,
            RequestOptions options) {
        return Observable.defer(() -> {
            try {
                if (collection == null) {
                    throw new IllegalArgumentException("collection");
                }

                logger.debug("Replacing a Collection. id: [{}]", collection.getId());
                validateResource(collection);

                String path = Utils.joinPath(collection.getSelfLink(), null);
                Map<String, String> requestHeaders = this.getRequestHeaders(options);

                RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.Replace,
                        ResourceType.DocumentCollection, path, collection, requestHeaders);

                return this.doReplace(request).map(response -> toResourceResponse(response, DocumentCollection.class));

            } catch (Exception e) {
                logger.debug("Failure in replacing a collection. due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });
    }

    @Override
    public Observable<ResourceResponse<DocumentCollection>> deleteCollection(String collectionLink,
            RequestOptions options) {
        return Observable.defer(() -> {
            try {
                if (StringUtils.isEmpty(collectionLink)) {
                    throw new IllegalArgumentException("collectionLink");
                }

                logger.debug("Deleting a Collection. collectionLink: [{}]", collectionLink);
                String path = Utils.joinPath(collectionLink, null);
                Map<String, String> requestHeaders = this.getRequestHeaders(options);
                RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.Delete,
                        ResourceType.DocumentCollection, path, requestHeaders);
                return this.doDelete(request).map(response -> toResourceResponse(response, DocumentCollection.class));

            } catch (Exception e) {
                logger.debug("Failure in deleting a collection, due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });
    }

    private Observable<DocumentServiceResponse> doDelete(RxDocumentServiceRequest request)
            throws DocumentClientException {

        Observable<DocumentServiceResponse> responseObservable = Observable.defer(() -> {
            try {
                return this.gatewayProxy.doDelete(request).doOnNext(response -> {
                    if (request.getResourceType() != ResourceType.DocumentCollection) {
                        this.captureSessionToken(request, response);
                    } else {
                        this.clearToken(request, response);
                    }
                });
            } catch (Exception e) {
                return Observable.error(e);
            }
        }).retryWhen(createExecuteRequestRetryHandler(request));

        return createPutMoreContentObservable(request, HttpConstants.HttpMethods.DELETE)
                .doOnNext(req -> this.applySessionToken(request)).flatMap(req -> responseObservable);
    }

    private Observable<DocumentServiceResponse> doRead(RxDocumentServiceRequest request)
            throws DocumentClientException {

        Observable<DocumentServiceResponse> responseObservable = Observable.defer(() -> {
            try {
                return this.gatewayProxy.processMessage(request).doOnNext(response -> {
                    this.captureSessionToken(request, response);
                });
            } catch (Exception e) {
                return Observable.error(e);
            }
        }).retryWhen(createExecuteRequestRetryHandler(request));

        return createPutMoreContentObservable(request, HttpConstants.HttpMethods.GET)
                .doOnNext(req -> this.applySessionToken(request)).flatMap(req -> responseObservable);
    }

    private Observable<DocumentServiceResponse> doReadFeed(RxDocumentServiceRequest request)
            throws DocumentClientException {
        Observable<DocumentServiceResponse> responseObservable = Observable.defer(() -> {
            try {
                return this.gatewayProxy.processMessage(request).doOnNext(response -> {
                    this.captureSessionToken(request, response);
                });
            } catch (Exception e) {
                return Observable.error(e);
            }
        }).retryWhen(createExecuteRequestRetryHandler(request));

        return createPutMoreContentObservable(request, HttpConstants.HttpMethods.GET).doOnNext(req -> {
            if (!request.isChangeFeedRequest()) {
                this.applySessionToken(request);
            }
        }).flatMap(req -> responseObservable);
    }

    @Override
    public Observable<ResourceResponse<DocumentCollection>> readCollection(String collectionLink,
            RequestOptions options) {

        return Observable.defer(() -> {
            // we are using an observable factory here
            // observable will be created fresh upon subscription
            // this is to ensure we capture most up to date information (e.g.,
            // session)
            try {
                if (StringUtils.isEmpty(collectionLink)) {
                    throw new IllegalArgumentException("collectionLink");
                }

                logger.debug("Reading a Collection. collectionLink: [{}]", collectionLink);
                String path = Utils.joinPath(collectionLink, null);
                Map<String, String> requestHeaders = this.getRequestHeaders(options);
                RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.Read,
                        ResourceType.DocumentCollection, path, requestHeaders);

                return this.doRead(request).map(response -> toResourceResponse(response, DocumentCollection.class));
            } catch (Exception e) {
                // this is only in trace level to capture what's going on
                logger.debug("Failure in reading a collection, due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });
    }

    @Override
    public Observable<FeedResponsePage<DocumentCollection>> readCollections(String databaseLink, FeedOptions options) {
        
        if (StringUtils.isEmpty(databaseLink)) {
            throw new IllegalArgumentException("databaseLink");
        }
        
        if (options == null)
            options = new FeedOptions();
        
        return getObservableFeedResponsePage(databaseLink, options, DocumentCollection.class,
                new Func4<String, String, Map<String, String>, RxDocumentClientImpl, Observable<FeedResponsePage<DocumentCollection>>>() {

                    @Override
                    public Observable<FeedResponsePage<DocumentCollection>> call(String token, String databaseLink,
                            Map<String, String> requestHeaders, RxDocumentClientImpl client) {
                        if (token != null)
                            requestHeaders.put(HttpConstants.HttpHeaders.CONTINUATION, token);

                        RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.ReadFeed,
                                ResourceType.DocumentCollection, Utils.joinPath(databaseLink, Paths.COLLECTIONS_PATH_SEGMENT), requestHeaders);
                        try {
                            return client.doReadFeed(request).map(response -> toFeedResponsePage(response, DocumentCollection.class));
                        } catch (DocumentClientException e) {
                            return Observable.error(e);
                        }
                    }
                });
    }

    @Override
    public Observable<FeedResponsePage<DocumentCollection>> queryCollections(String databaseLink, String query,
            FeedOptions options) {
        return this.rxWrapperClient.queryCollections(databaseLink, query, options);
    }

    @Override
    public Observable<FeedResponsePage<DocumentCollection>> queryCollections(String databaseLink,
            SqlQuerySpec querySpec, FeedOptions options) {
        return this.rxWrapperClient.queryCollections(databaseLink, querySpec, options);
    }

    private String getTargetDocumentCollectionLink(String collectionLink, Object document) {
        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }
        if (document == null) {
            throw new IllegalArgumentException("document");
        }

        String documentCollectionLink = collectionLink;
        if (Utils.isDatabaseLink(collectionLink)) {

            // TODO: not supported yet

            // // Gets the partition resolver(if it exists) for the specified
            // database link
            // PartitionResolver partitionResolver =
            // this.getPartitionResolver(collectionLink);
            //
            // // If the partition resolver exists, get the collection to which
            // the Create/Upsert should be directed using the partition key
            // if (partitionResolver != null) {
            // documentCollectionLink =
            // partitionResolver.resolveForCreate(document);
            // } else {
            // throw new
            // IllegalArgumentException(PartitionResolverErrorMessage);
            // }
        }

        return documentCollectionLink;
    }

    private static String serializeProcedureParams(Object[] objectArray) {
        String[] stringArray = new String[objectArray.length];

        for (int i = 0; i < objectArray.length; ++i) {
            Object object = objectArray[i];
            if (object instanceof JsonSerializable) {
                stringArray[i] = ((JsonSerializable) object).toJson();
            } else if (object instanceof JSONObject) {
                stringArray[i] = object.toString();
            } else {

                // POJO, number, String or Boolean
                try {
                    stringArray[i] = mapper.writeValueAsString(object);
                } catch (IOException e) {
                    throw new IllegalArgumentException("Can't serialize the object into the json string", e);
                }
            }
        }

        return String.format("[%s]", StringUtils.join(stringArray, ","));
    }

    private static void validateResource(Resource resource) {
        BridgeInternal.validateResource(resource);
    }

    private Map<String, String> getRequestHeaders(RequestOptions options) {
        return BridgeInternal.getRequestHeaders(options);
    }

    private Map<String, String> getFeedHeaders(FeedOptionsBase options) {
        return BridgeInternal.getFeedHeaders(options);
    }

    private Map<String, String> getMediaHeaders(MediaOptions options) {
        Map<String, String> requestHeaders = new HashMap<String, String>();

        if (options == null || options.getContentType().isEmpty()) {
            requestHeaders.put(HttpConstants.HttpHeaders.CONTENT_TYPE, RuntimeConstants.MediaTypes.OCTET_STREAM);
        }

        if (options != null) {
            if (!options.getContentType().isEmpty()) {
                requestHeaders.put(HttpConstants.HttpHeaders.CONTENT_TYPE, options.getContentType());
            }

            if (!options.getSlug().isEmpty()) {
                requestHeaders.put(HttpConstants.HttpHeaders.SLUG, options.getSlug());
            }
        }
        return requestHeaders;
    }

    private void addPartitionKeyInformation(RxDocumentServiceRequest request, Document document,
            RequestOptions options) {
        addPartitionKeyInformation(request, document, options, this.collectionCache.resolveCollection(request));
    }

    private void addPartitionKeyInformation(RxDocumentServiceRequest request, Document document, RequestOptions options,
            DocumentCollection collection) {
        BridgeInternal.addPartitionKeyInformation(request, document, options, collection);
    }

    private RxDocumentServiceRequest getCreateDocumentRequest(String documentCollectionLink, Object document,
            RequestOptions options, boolean disableAutomaticIdGeneration, OperationType operationType) {

        if (StringUtils.isEmpty(documentCollectionLink)) {
            throw new IllegalArgumentException("documentCollectionLink");
        }
        if (document == null) {
            throw new IllegalArgumentException("document");
        }

        Document typedDocument = documentFromObject(document);

        RxDocumentClientImpl.validateResource(typedDocument);

        if (typedDocument.getId() == null && !disableAutomaticIdGeneration) {
            // We are supposed to use GUID. Basically UUID is the same as GUID
            // when represented as a string.
            typedDocument.setId(UUID.randomUUID().toString());
        }
        String path = Utils.joinPath(documentCollectionLink, Paths.DOCUMENTS_PATH_SEGMENT);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);

        RxDocumentServiceRequest request = RxDocumentServiceRequest.create(operationType, ResourceType.Document, path,
                typedDocument, requestHeaders);

        // NOTE: if the collection is not currently cached this will be a
        // blocking call
        DocumentCollection collection = this.collectionCache.resolveCollection(request);

        this.addPartitionKeyInformation(request, typedDocument, options, collection);
        return request;
    }

    private void putMoreContentIntoDocumentServiceRequest(RxDocumentServiceRequest request, String httpMethod) {
        if (this.masterKey != null) {
            final Date currentTime = new Date();
            final SimpleDateFormat sdf = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US);
            sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
            String xDate = sdf.format(currentTime);

            request.getHeaders().put(HttpConstants.HttpHeaders.X_DATE, xDate);
        }

        if (this.masterKey != null || this.resourceTokens != null) {
            String resourceName = request.getResourceFullName();
            String authorization = this.getAuthorizationToken(resourceName, request.getPath(),
                    request.getResourceType(), httpMethod, request.getHeaders(), this.masterKey, this.resourceTokens);
            try {
                authorization = URLEncoder.encode(authorization, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new IllegalStateException("Failed to encode authtoken.", e);
            }
            request.getHeaders().put(HttpConstants.HttpHeaders.AUTHORIZATION, authorization);
        }

        if ((HttpConstants.HttpMethods.POST.equals(httpMethod) || HttpConstants.HttpMethods.PUT.equals(httpMethod))
                && !request.getHeaders().containsKey(HttpConstants.HttpHeaders.CONTENT_TYPE)) {
            request.getHeaders().put(HttpConstants.HttpHeaders.CONTENT_TYPE, RuntimeConstants.MediaTypes.JSON);
        }

        if (!request.getHeaders().containsKey(HttpConstants.HttpHeaders.ACCEPT)) {
            request.getHeaders().put(HttpConstants.HttpHeaders.ACCEPT, RuntimeConstants.MediaTypes.JSON);
        }
    }

    private String getAuthorizationToken(String resourceOrOwnerId, String path, ResourceType resourceType,
            String requestVerb, Map<String, String> headers, String masterKey, Map<String, String> resourceTokens) {
        if (masterKey != null) {
            return this.authorizationTokenProvider.generateKeyAuthorizationSignature(requestVerb, resourceOrOwnerId,
                    resourceType, headers);
        } else {
            assert resourceTokens != null;
            return this.authorizationTokenProvider.getAuthorizationTokenUsingResourceTokens(resourceTokens, path,
                    resourceOrOwnerId);
        }
    }

    private void applySessionToken(RxDocumentServiceRequest request) {
        Map<String, String> headers = request.getHeaders();
        if (headers != null && !StringUtils.isEmpty(headers.get(HttpConstants.HttpHeaders.SESSION_TOKEN))) {
            return; // User is explicitly controlling the session.
        }

        String requestConsistency = request.getHeaders().get(HttpConstants.HttpHeaders.CONSISTENCY_LEVEL);
        boolean sessionConsistency = this.consistencyLevel == ConsistencyLevel.Session
                || (!StringUtils.isEmpty(requestConsistency)
                        && StringUtils.equalsIgnoreCase(requestConsistency, ConsistencyLevel.Session.toString()));
        if (!sessionConsistency) {
            return; // Only apply the session token in case of session consistency
        }

        // Apply the ambient session.
        if (!StringUtils.isEmpty(request.getResourceAddress())) {
            String sessionToken = this.sessionContainer.resolveGlobalSessionToken(request);

            if (!StringUtils.isEmpty(sessionToken)) {
                headers.put(HttpConstants.HttpHeaders.SESSION_TOKEN, sessionToken);
            }
        }
    }

    void captureSessionToken(RxDocumentServiceRequest request, DocumentServiceResponse response) {
        this.sessionContainer.setSessionToken(request, response);
    }

    void clearToken(RxDocumentServiceRequest request, DocumentServiceResponse response) {
        this.sessionContainer.clearToken(request);
    }

    private Observable<DocumentServiceResponse> doCreate(RxDocumentServiceRequest request) {

        Observable<DocumentServiceResponse> responseObservable = Observable.defer(() -> {
            try {
                return this.gatewayProxy.processMessage(request).doOnNext(response -> {
                    this.captureSessionToken(request, response);
                });
            } catch (Exception e) {
                return Observable.error(e);
            }
        }).retryWhen(createExecuteRequestRetryHandler(request));

        return createPutMoreContentObservable(request, HttpConstants.HttpMethods.POST)
                .doOnNext(r -> applySessionToken(request)).flatMap(req -> responseObservable);

    }

    /**
     * Creates an observable which does the CPU intensive operation of generating
     * authentication token and putting more content in the request
     * 
     * This observable runs on computationScheduler
     * 
     * @param request
     * @param method
     * @return
     */
    private Observable<Object> createPutMoreContentObservable(RxDocumentServiceRequest request, String method) {
        return Observable.create(s -> {
            try {
                putMoreContentIntoDocumentServiceRequest(request, method);
                s.onNext(request);
                s.onCompleted();
            } catch (Exception e) {
                s.onError(e);
            }
        }).subscribeOn(this.computationScheduler);
    }

    private Observable<DocumentServiceResponse> doUpsert(RxDocumentServiceRequest request) {

        Observable<DocumentServiceResponse> responseObservable = Observable.defer(() -> {
            try {
                return this.gatewayProxy.processMessage(request).doOnNext(response -> {
                    this.captureSessionToken(request, response);
                });
            } catch (Exception e) {
                return Observable.error(e);
            }
        }).retryWhen(createExecuteRequestRetryHandler(request));

        return createPutMoreContentObservable(request, HttpConstants.HttpMethods.POST).doOnNext(r -> {
            applySessionToken(request);
            Map<String, String> headers = request.getHeaders();
            // headers can never be null, since it will be initialized even when no
            // request options are specified,
            // hence using assertion here instead of exception, being in the private
            // method
            assert (headers != null);
            headers.put(HttpConstants.HttpHeaders.IS_UPSERT, "true");

        }).flatMap(req -> responseObservable);
    }

    private Observable<DocumentServiceResponse> doReplace(RxDocumentServiceRequest request) {

        Observable<DocumentServiceResponse> responseObservable = Observable.defer(() -> {
            try {
                return this.gatewayProxy.doReplace(request).doOnNext(response -> {
                    this.captureSessionToken(request, response);
                });
            } catch (Exception e) {
                return Observable.error(e);
            }
        }).retryWhen(createExecuteRequestRetryHandler(request));

        return createPutMoreContentObservable(request, HttpConstants.HttpMethods.PUT)
                .doOnNext(r -> applySessionToken(request)).flatMap(req -> responseObservable);

    }

    @Override
    public Observable<ResourceResponse<Document>> createDocument(String collectionLink, Object document,
            RequestOptions options, boolean disableAutomaticIdGeneration) {

        return Observable.defer(() -> {

            try {
                logger.debug("Creating a Document. collectionLink: [{}]", collectionLink);

                final String documentCollectionLink = this.getTargetDocumentCollectionLink(collectionLink, document);
                final Object documentLocal = document;
                final RequestOptions optionsLocal = options;
                final boolean disableAutomaticIdGenerationLocal = disableAutomaticIdGeneration;
                final boolean shouldRetry = options == null || options.getPartitionKey() == null;
                Observable<ResourceResponse<Document>> createObservable = Observable.defer(() -> {
                    RxDocumentServiceRequest request = getCreateDocumentRequest(documentCollectionLink, documentLocal,
                            optionsLocal, disableAutomaticIdGenerationLocal, OperationType.Create);

                    Observable<DocumentServiceResponse> responseObservable = this.doCreate(request);
                    return responseObservable
                            .map(serviceResponse -> toResourceResponse(serviceResponse, Document.class));
                });

                if (shouldRetry) {
                    CreateDocumentRetryHandler createDocumentRetryHandler = new CreateDocumentRetryHandler(
                            this.collectionCache, documentCollectionLink);
                    return createObservable.retryWhen(RetryFunctionFactory.from(createDocumentRetryHandler));
                } else {
                    return createObservable;
                }

            } catch (Exception e) {
                logger.debug("Failure in creating a document due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });
    }

    @Override
    public Observable<ResourceResponse<Document>> upsertDocument(String collectionLink, Object document,
            RequestOptions options, boolean disableAutomaticIdGeneration) {
        return Observable.defer(() -> {
            try {
                logger.debug("Upserting a Document. collectionLink: [{}]", collectionLink);
                final String documentCollectionLink = this.getTargetDocumentCollectionLink(collectionLink, document);
                final Object documentLocal = document;
                final RequestOptions optionsLocal = options;
                final boolean disableAutomaticIdGenerationLocal = disableAutomaticIdGeneration;
                final boolean shouldRetry = options == null || options.getPartitionKey() == null;

                Observable<ResourceResponse<Document>> upsertObservable = Observable.defer(() -> {

                    RxDocumentServiceRequest request = getCreateDocumentRequest(documentCollectionLink, documentLocal,
                            optionsLocal, disableAutomaticIdGenerationLocal, OperationType.Upsert);

                    Observable<DocumentServiceResponse> responseObservable = this.doUpsert(request);
                    return responseObservable
                            .map(serviceResponse -> toResourceResponse(serviceResponse, Document.class));
                });

                if (shouldRetry) {
                    CreateDocumentRetryHandler createDocumentRetryHandler = new CreateDocumentRetryHandler(
                            this.collectionCache, documentCollectionLink);
                    return upsertObservable.retryWhen(RetryFunctionFactory.from(createDocumentRetryHandler));
                } else {
                    return upsertObservable;
                }

            } catch (Exception e) {
                logger.debug("Failure in upserting a document due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });
    }

    @Override
    public Observable<ResourceResponse<Document>> replaceDocument(String documentLink, Object document,
            RequestOptions options) {
        return Observable.defer(() -> {

            try {
                if (StringUtils.isEmpty(documentLink)) {
                    throw new IllegalArgumentException("documentLink");
                }

                if (document == null) {
                    throw new IllegalArgumentException("document");
                }

                Document typedDocument = documentFromObject(document);

                return this.replaceDocumentInternal(documentLink, typedDocument, options);

            } catch (Exception e) {
                logger.debug("Failure in replacing a document due to [{}]", e.getMessage());
                return Observable.error(e);
            }
        });
    }

    @Override
    public Observable<ResourceResponse<Document>> replaceDocument(Document document, RequestOptions options) {

        return Observable.defer(() -> {

            try {
                if (document == null) {
                    throw new IllegalArgumentException("document");
                }

                return this.replaceDocumentInternal(document.getSelfLink(), document, options);

            } catch (Exception e) {
                logger.debug("Failure in replacing a database due to [{}]", e.getMessage());
                return Observable.error(e);
            }
        });
    }

    private Observable<ResourceResponse<Document>> replaceDocumentInternal(String documentLink, Document document,
            RequestOptions options) throws DocumentClientException {

        if (document == null) {
            throw new IllegalArgumentException("document");
        }

        logger.debug("Replacing a Document. documentLink: [{}]", documentLink);
        final String documentCollectionName = Utils.getCollectionName(documentLink);
        final String documentCollectionLink = this.getTargetDocumentCollectionLink(documentCollectionName, document);
        final String path = Utils.joinPath(documentLink, null);
        final Map<String, String> requestHeaders = getRequestHeaders(options);
        final RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.Replace,
                ResourceType.Document, path, document, requestHeaders);

        final boolean shouldRetry = options == null || options.getPartitionKey() == null;

        // NOTE: if the collection is not cached this will block till collection
        // is retrieved
        DocumentCollection collection = this.collectionCache.resolveCollection(request);

        this.addPartitionKeyInformation(request, document, options, collection);
        validateResource(document);

        Observable<ResourceResponse<Document>> resourceResponseObs = this.doReplace(request)
                .map(resp -> toResourceResponse(resp, Document.class));

        if (shouldRetry) {
            CreateDocumentRetryHandler createDocumentRetryHandler = new CreateDocumentRetryHandler(null,
                    documentCollectionLink);
            return resourceResponseObs.retryWhen(RetryFunctionFactory.from(createDocumentRetryHandler));
        } else {
            return resourceResponseObs;
        }
    }

    @Override
    public Observable<ResourceResponse<Document>> deleteDocument(String documentLink, RequestOptions options) {
        return Observable.defer(() -> {

            try {
                if (StringUtils.isEmpty(documentLink)) {
                    throw new IllegalArgumentException("documentLink");
                }

                logger.debug("Deleting a Document. documentLink: [{}]", documentLink);
                String path = Utils.joinPath(documentLink, null);
                Map<String, String> requestHeaders = this.getRequestHeaders(options);
                RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.Delete,
                        ResourceType.Document, path, requestHeaders);

                // NOTE: if collection is not cached, this will block till
                // collection is retrieved
                DocumentCollection collection = this.collectionCache.resolveCollection(request);

                this.addPartitionKeyInformation(request, null, options, collection);

                Observable<DocumentServiceResponse> responseObservable = this.doDelete(request);
                return responseObservable.map(serviceResponse -> toResourceResponse(serviceResponse, Document.class));

            } catch (Exception e) {
                logger.debug("Failure in deleting a document due to [{}]", e.getMessage());
                return Observable.error(e);
            }
        });
    }

    @Override
    public Observable<ResourceResponse<Document>> readDocument(String documentLink, RequestOptions options) {
        return Observable.defer(() -> {

            try {
                if (StringUtils.isEmpty(documentLink)) {
                    throw new IllegalArgumentException("documentLink");
                }

                logger.debug("Reading a Document. documentLink: [{}]", documentLink);
                String path = Utils.joinPath(documentLink, null);
                Map<String, String> requestHeaders = this.getRequestHeaders(options);
                RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.Read,
                        ResourceType.Document, path, requestHeaders);

                // NOTE: if the collection is not cached, this will block till
                // collection is retrieved
                DocumentCollection collection = this.collectionCache.resolveCollection(request);

                this.addPartitionKeyInformation(request, null, options, collection);

                Observable<DocumentServiceResponse> responseObservable = this.doRead(request);
                return responseObservable.map(serviceResponse -> toResourceResponse(serviceResponse, Document.class));

            } catch (Exception e) {
                logger.debug("Failure in reading a document due to [{}]", e.getMessage());
                return Observable.error(e);
            }
        });
    }

    @Override
    public Observable<FeedResponsePage<Document>> readDocuments(String collectionLink, FeedOptions options) {
        
        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }
        
        if (options == null)
            options = new FeedOptions();
        
        RequestOptions requestOptions = new RequestOptions();
        requestOptions.setPartitionKey(options.getPartitionKey());
        
        //currently works only for non partitioned collections
        return getObservableFeedResponsePage(collectionLink, options, Document.class,
                new Func4<String, String, Map<String, String>, RxDocumentClientImpl, Observable<FeedResponsePage<Document>>>() {

                    @Override
                    public Observable<FeedResponsePage<Document>> call(String token, String collectionLink,
                            Map<String, String> requestHeaders, RxDocumentClientImpl client) {
                        if (token != null)
                            requestHeaders.put(HttpConstants.HttpHeaders.CONTINUATION, token);

                        RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.ReadFeed,
                                ResourceType.Document, Utils.joinPath(collectionLink, Paths.DOCUMENTS_PATH_SEGMENT), requestHeaders);

                        DocumentCollection collection = client.collectionCache.resolveCollection(request);
                        client.addPartitionKeyInformation(request, null, requestOptions, collection);
                        
                        try {
                            return client.doReadFeed(request)
                                    .map(response -> toFeedResponsePage(response, Document.class));
                        } catch (DocumentClientException e) {
                            return Observable.error(e);
                        }
                    }
                });
    }

    @Override
    public Observable<FeedResponsePage<Document>> queryDocuments(String collectionLink, String query,
            FeedOptions options) {
        return this.rxWrapperClient.queryDocuments(collectionLink, query, options);
    }

    @Override
    public Observable<FeedResponsePage<Document>> queryDocuments(String collectionLink, String query,
            FeedOptions options, Object partitionKey) {
        return this.rxWrapperClient.queryDocuments(collectionLink, query, options, partitionKey);
    }

    @Override
    public Observable<FeedResponsePage<Document>> queryDocuments(String collectionLink, SqlQuerySpec querySpec,
            FeedOptions options) {
        return this.rxWrapperClient.queryDocuments(collectionLink, querySpec, options);
    }

    @Override
    public Observable<FeedResponsePage<Document>> queryDocuments(String collectionLink, SqlQuerySpec querySpec,
            FeedOptions options, Object partitionKey) {
        return this.rxWrapperClient.queryDocuments(collectionLink, querySpec, options, partitionKey);
    }

    @Override
    public Observable<FeedResponsePage<Document>> queryDocumentChangeFeed(final String collectionLink,
            final ChangeFeedOptions changeFeedOptions) {
        return this.rxWrapperClient.queryDocumentChangeFeed(collectionLink, changeFeedOptions);
    }

    @Override
    public Observable<FeedResponsePage<PartitionKeyRange>> readPartitionKeyRanges(final String collectionLink,
            FeedOptions options) {
        
        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }
        
        if (options == null)
            options = new FeedOptions();
        
        return getObservableFeedResponsePage(collectionLink, options, PartitionKeyRange.class,
                new Func4<String, String, Map<String, String>, RxDocumentClientImpl, Observable<FeedResponsePage<PartitionKeyRange>>>() {

                    @Override
                    public Observable<FeedResponsePage<PartitionKeyRange>> call(String token, String collectionLink,
                            Map<String, String> requestHeaders, RxDocumentClientImpl client) {
                        if (token != null)
                            requestHeaders.put(HttpConstants.HttpHeaders.CONTINUATION, token);

                        RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.ReadFeed,
                                ResourceType.PartitionKeyRange, Utils.joinPath(collectionLink, Paths.PARTITION_KEY_RANGE_PATH_SEGMENT), requestHeaders);

                        // Add partitionkey info

                        try {
                            return client.doReadFeed(request)
                                    .map(response -> toFeedResponsePage(response, PartitionKeyRange.class));
                        } catch (DocumentClientException e) {
                            return Observable.error(e);
                        }
                    }
                });
    }

    private RxDocumentServiceRequest getStoredProcedureRequest(String collectionLink, StoredProcedure storedProcedure,
            RequestOptions options, OperationType operationType) {
        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }
        if (storedProcedure == null) {
            throw new IllegalArgumentException("storedProcedure");
        }

        validateResource(storedProcedure);

        String path = Utils.joinPath(collectionLink, Paths.STORED_PROCEDURES_PATH_SEGMENT);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        RxDocumentServiceRequest request = RxDocumentServiceRequest.create(operationType, ResourceType.StoredProcedure,
                path, storedProcedure, requestHeaders);
        return request;
    }

    private RxDocumentServiceRequest getUserDefinedFunctionRequest(String collectionLink, UserDefinedFunction udf,
            RequestOptions options, OperationType operationType) {
        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }
        if (udf == null) {
            throw new IllegalArgumentException("udf");
        }

        validateResource(udf);

        String path = Utils.joinPath(collectionLink, Paths.USER_DEFINED_FUNCTIONS_PATH_SEGMENT);
        Map<String, String> requestHeaders = this.getRequestHeaders(options);
        RxDocumentServiceRequest request = RxDocumentServiceRequest.create(operationType,
                ResourceType.UserDefinedFunction, path, udf, requestHeaders);
        return request;
    }

    @Override
    public Observable<ResourceResponse<StoredProcedure>> createStoredProcedure(String collectionLink,
            StoredProcedure storedProcedure, RequestOptions options) {

        return Observable.defer(() -> {
            // we are using an observable factory here
            // observable will be created fresh upon subscription
            // this is to ensure we capture most up to date information (e.g.,
            // session)
            try {

                logger.debug("Creating a StoredProcedure. collectionLink: [{}], storedProcedure id [{}]",
                        collectionLink, storedProcedure.getId());
                RxDocumentServiceRequest request = getStoredProcedureRequest(collectionLink, storedProcedure, options,
                        OperationType.Create);

                return this.doCreate(request).map(response -> toResourceResponse(response, StoredProcedure.class));

            } catch (Exception e) {
                // this is only in trace level to capture what's going on
                logger.debug("Failure in creating a StoredProcedure due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });
    }

    @Override
    public Observable<ResourceResponse<StoredProcedure>> upsertStoredProcedure(String collectionLink,
            StoredProcedure storedProcedure, RequestOptions options) {
        return Observable.defer(() -> {
            // we are using an observable factory here
            // observable will be created fresh upon subscription
            // this is to ensure we capture most up to date information (e.g.,
            // session)
            try {

                logger.debug("Upserting a StoredProcedure. collectionLink: [{}], storedProcedure id [{}]",
                        collectionLink, storedProcedure.getId());
                RxDocumentServiceRequest request = getStoredProcedureRequest(collectionLink, storedProcedure, options,
                        OperationType.Upsert);

                return this.doUpsert(request).map(response -> toResourceResponse(response, StoredProcedure.class));

            } catch (Exception e) {
                // this is only in trace level to capture what's going on
                logger.debug("Failure in upserting a StoredProcedure due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });
    }

    @Override
    public Observable<ResourceResponse<StoredProcedure>> replaceStoredProcedure(StoredProcedure storedProcedure,
            RequestOptions options) {
        return Observable.defer(() -> {
            try {

                if (storedProcedure == null) {
                    throw new IllegalArgumentException("storedProcedure");
                }
                logger.debug("Replacing a StoredProcedure. storedProcedure id [{}]", storedProcedure.getId());

                RxDocumentClientImpl.validateResource(storedProcedure);

                String path = Utils.joinPath(storedProcedure.getSelfLink(), null);
                Map<String, String> requestHeaders = getRequestHeaders(options);
                RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.Replace,
                        ResourceType.StoredProcedure, path, storedProcedure, requestHeaders);
                return this.doReplace(request).map(response -> toResourceResponse(response, StoredProcedure.class));

            } catch (Exception e) {
                logger.debug("Failure in replacing a StoredProcedure due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });
    }

    @Override
    public Observable<ResourceResponse<StoredProcedure>> deleteStoredProcedure(String storedProcedureLink,
            RequestOptions options) {
        return Observable.defer(() -> {
            // we are using an observable factory here
            // observable will be created fresh upon subscription
            // this is to ensure we capture most up to date information (e.g.,
            // session)
            try {

                if (StringUtils.isEmpty(storedProcedureLink)) {
                    throw new IllegalArgumentException("storedProcedureLink");
                }

                logger.debug("Deleting a StoredProcedure. storedProcedureLink [{}]", storedProcedureLink);
                String path = Utils.joinPath(storedProcedureLink, null);
                Map<String, String> requestHeaders = this.getRequestHeaders(options);
                RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.Delete,
                        ResourceType.StoredProcedure, path, requestHeaders);

                return this.doDelete(request).map(response -> toResourceResponse(response, StoredProcedure.class));

            } catch (Exception e) {
                // this is only in trace level to capture what's going on
                logger.debug("Failure in deleting a StoredProcedure due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });
    }

    @Override
    public Observable<ResourceResponse<StoredProcedure>> readStoredProcedure(String storedProcedureLink,
            RequestOptions options) {

        return Observable.defer(() -> {
            // we are using an observable factory here
            // observable will be created fresh upon subscription
            // this is to ensure we capture most up to date information (e.g.,
            // session)
            try {

                if (StringUtils.isEmpty(storedProcedureLink)) {
                    throw new IllegalArgumentException("storedProcedureLink");
                }

                logger.debug("Reading a StoredProcedure. storedProcedureLink [{}]", storedProcedureLink);
                String path = Utils.joinPath(storedProcedureLink, null);
                Map<String, String> requestHeaders = this.getRequestHeaders(options);
                RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.Read,
                        ResourceType.StoredProcedure, path, requestHeaders);

                return this.doRead(request).map(response -> toResourceResponse(response, StoredProcedure.class));

            } catch (Exception e) {
                // this is only in trace level to capture what's going on
                logger.debug("Failure in reading a StoredProcedure due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });

    }

    @Override
    public Observable<FeedResponsePage<StoredProcedure>> readStoredProcedures(String collectionLink,
            FeedOptions options) {
        
        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }
        
        if (options == null)
            options = new FeedOptions();
        
        return getObservableFeedResponsePage(collectionLink, options, StoredProcedure.class,
                new Func4<String, String, Map<String, String>, RxDocumentClientImpl, Observable<FeedResponsePage<StoredProcedure>>>() {

                    @Override
                    public Observable<FeedResponsePage<StoredProcedure>> call(String token, String collectionLink,
                            Map<String, String> requestHeaders, RxDocumentClientImpl client) {
                        if (token != null)
                            requestHeaders.put(HttpConstants.HttpHeaders.CONTINUATION, token);

                        RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.ReadFeed,
                                ResourceType.StoredProcedure, Utils.joinPath(collectionLink, Paths.STORED_PROCEDURES_PATH_SEGMENT), requestHeaders);
                        try {
                            return client.doReadFeed(request).map(response -> toFeedResponsePage(response, StoredProcedure.class));
                        } catch (DocumentClientException e) {
                            return Observable.error(e);
                        }
                    }
                });
    }

    @Override
    public Observable<FeedResponsePage<StoredProcedure>> queryStoredProcedures(String collectionLink, String query,
            FeedOptions options) {
        return this.rxWrapperClient.queryStoredProcedures(collectionLink, query, options);
    }

    @Override
    public Observable<FeedResponsePage<StoredProcedure>> queryStoredProcedures(String collectionLink,
            SqlQuerySpec querySpec, FeedOptions options) {
        return this.rxWrapperClient.queryStoredProcedures(collectionLink, querySpec, options);
    }

    @Override
    public Observable<StoredProcedureResponse> executeStoredProcedure(String storedProcedureLink,
            Object[] procedureParams) {
        return this.executeStoredProcedure(storedProcedureLink, null, procedureParams);
    }

    @Override
    public Observable<StoredProcedureResponse> executeStoredProcedure(String storedProcedureLink,
            RequestOptions options, Object[] procedureParams) {

        return Observable.defer(() -> {
            try {
                logger.debug("Executing a StoredProcedure. storedProcedureLink [{}]", storedProcedureLink);
                String path = Utils.joinPath(storedProcedureLink, null);

                Map<String, String> requestHeaders = new HashMap<String, String>();
                requestHeaders.put(HttpConstants.HttpHeaders.ACCEPT, RuntimeConstants.MediaTypes.JSON);
                if (options != null) {
                    if (options.getPartitionKey() != null) {
                        requestHeaders.put(HttpConstants.HttpHeaders.PARTITION_KEY,
                                options.getPartitionKey().toString());
                    }
                    if (options.isScriptLoggingEnabled()) {
                        requestHeaders.put(HttpConstants.HttpHeaders.SCRIPT_ENABLE_LOGGING, String.valueOf(true));
                    }
                }

                RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.ExecuteJavaScript,
                        ResourceType.StoredProcedure, path,
                        procedureParams != null ? RxDocumentClientImpl.serializeProcedureParams(procedureParams) : "",
                        requestHeaders);
                this.addPartitionKeyInformation(request, null, options);
                return this.doCreate(request).map(response -> toStoredProcedureResponse(response));

            } catch (Exception e) {
                logger.debug("Failure in executing a StoredProcedure due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });

    }

    @Override
    public Observable<ResourceResponse<Trigger>> createTrigger(String collectionLink, Trigger trigger,
            RequestOptions options) {

        return Observable.defer(() -> {
            try {

                logger.debug("Creating a Trigger. collectionLink [{}], trigger id [{}]", collectionLink,
                        trigger.getId());
                RxDocumentServiceRequest request = getTriggerRequest(collectionLink, trigger, options,
                        OperationType.Create);
                return this.doCreate(request).map(response -> toResourceResponse(response, Trigger.class));

            } catch (Exception e) {
                logger.debug("Failure in creating a Trigger due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });

    }

    @Override
    public Observable<ResourceResponse<Trigger>> upsertTrigger(String collectionLink, Trigger trigger,
            RequestOptions options) {

        return Observable.defer(() -> {
            try {

                logger.debug("Upserting a Trigger. collectionLink [{}], trigger id [{}]", collectionLink,
                        trigger.getId());
                RxDocumentServiceRequest request = getTriggerRequest(collectionLink, trigger, options,
                        OperationType.Upsert);
                return this.doUpsert(request).map(response -> toResourceResponse(response, Trigger.class));

            } catch (Exception e) {
                logger.debug("Failure in upserting a Trigger due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });

    }

    private RxDocumentServiceRequest getTriggerRequest(String collectionLink, Trigger trigger, RequestOptions options,
            OperationType operationType) {
        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }
        if (trigger == null) {
            throw new IllegalArgumentException("trigger");
        }

        RxDocumentClientImpl.validateResource(trigger);

        String path = Utils.joinPath(collectionLink, Paths.TRIGGERS_PATH_SEGMENT);
        Map<String, String> requestHeaders = getRequestHeaders(options);
        RxDocumentServiceRequest request = RxDocumentServiceRequest.create(operationType, ResourceType.Trigger, path,
                trigger, requestHeaders);
        return request;
    }

    @Override
    public Observable<ResourceResponse<Trigger>> replaceTrigger(Trigger trigger, RequestOptions options) {

        return Observable.defer(() -> {
            try {
                if (trigger == null) {
                    throw new IllegalArgumentException("trigger");
                }

                logger.debug("Replacing a Trigger. trigger id [{}]", trigger.getId());
                RxDocumentClientImpl.validateResource(trigger);

                String path = Utils.joinPath(trigger.getSelfLink(), null);
                Map<String, String> requestHeaders = getRequestHeaders(options);
                RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.Replace,
                        ResourceType.Trigger, path, trigger, requestHeaders);
                return this.doReplace(request).map(response -> toResourceResponse(response, Trigger.class));

            } catch (Exception e) {
                logger.debug("Failure in replacing a Trigger due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });
    }

    @Override
    public Observable<ResourceResponse<Trigger>> deleteTrigger(String triggerLink, RequestOptions options) {

        return Observable.defer(() -> {
            try {
                if (StringUtils.isEmpty(triggerLink)) {
                    throw new IllegalArgumentException("triggerLink");
                }

                logger.debug("Deleting a Trigger. triggerLink [{}]", triggerLink);
                String path = Utils.joinPath(triggerLink, null);
                Map<String, String> requestHeaders = getRequestHeaders(options);
                RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.Delete,
                        ResourceType.Trigger, path, requestHeaders);
                return this.doDelete(request).map(response -> toResourceResponse(response, Trigger.class));

            } catch (Exception e) {
                logger.debug("Failure in deleting a Trigger due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });
    }

    @Override
    public Observable<ResourceResponse<Trigger>> readTrigger(String triggerLink, RequestOptions options) {

        return Observable.defer(() -> {
            try {
                if (StringUtils.isEmpty(triggerLink)) {
                    throw new IllegalArgumentException("triggerLink");
                }

                logger.debug("Reading a Trigger. triggerLink [{}]", triggerLink);
                String path = Utils.joinPath(triggerLink, null);
                Map<String, String> requestHeaders = getRequestHeaders(options);
                RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.Read,
                        ResourceType.Trigger, path, requestHeaders);
                return this.doRead(request).map(response -> toResourceResponse(response, Trigger.class));

            } catch (Exception e) {
                logger.debug("Failure in reading a Trigger due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });
    }

    @Override
    public Observable<FeedResponsePage<Trigger>> readTriggers(String collectionLink, FeedOptions options) {
        
        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }
        
        if (options == null)
            options = new FeedOptions();
        
        return getObservableFeedResponsePage(collectionLink, options, Trigger.class,
                new Func4<String, String, Map<String, String>, RxDocumentClientImpl, Observable<FeedResponsePage<Trigger>>>() {

                    @Override
                    public Observable<FeedResponsePage<Trigger>> call(String token, String collectionLink,
                            Map<String, String> requestHeaders, RxDocumentClientImpl client) {
                        if (token != null)
                            requestHeaders.put(HttpConstants.HttpHeaders.CONTINUATION, token);

                        RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.ReadFeed,
                                ResourceType.Trigger, Utils.joinPath(collectionLink, Paths.TRIGGERS_PATH_SEGMENT), requestHeaders);
                        try {
                            return client.doReadFeed(request).map(response -> toFeedResponsePage(response, Trigger.class));
                        } catch (DocumentClientException e) {
                            return Observable.error(e);
                        }
                    }
                });
    }

    @Override
    public Observable<FeedResponsePage<Trigger>> queryTriggers(String collectionLink, String query,
            FeedOptions options) {
        return this.rxWrapperClient.queryTriggers(collectionLink, query, options);
    }

    @Override
    public Observable<FeedResponsePage<Trigger>> queryTriggers(String collectionLink, SqlQuerySpec querySpec,
            FeedOptions options) {
        return this.rxWrapperClient.queryTriggers(collectionLink, querySpec, options);
    }

    @Override
    public Observable<ResourceResponse<UserDefinedFunction>> createUserDefinedFunction(String collectionLink,
            UserDefinedFunction udf, RequestOptions options) {
        return Observable.defer(() -> {
            // we are using an observable factory here
            // observable will be created fresh upon subscription
            // this is to ensure we capture most up to date information (e.g.,
            // session)
            try {
                logger.debug("Creating a UserDefinedFunction. collectionLink [{}], udf id [{}]", collectionLink,
                        udf.getId());
                RxDocumentServiceRequest request = getUserDefinedFunctionRequest(collectionLink, udf, options,
                        OperationType.Create);

                return this.doCreate(request).map(response -> toResourceResponse(response, UserDefinedFunction.class));

            } catch (Exception e) {
                // this is only in trace level to capture what's going on
                logger.debug("Failure in creating a UserDefinedFunction due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });
    }

    @Override
    public Observable<ResourceResponse<UserDefinedFunction>> upsertUserDefinedFunction(String collectionLink,
            UserDefinedFunction udf, RequestOptions options) {
        return Observable.defer(() -> {
            // we are using an observable factory here
            // observable will be created fresh upon subscription
            // this is to ensure we capture most up to date information (e.g.,
            // session)
            try {
                logger.debug("Upserting a UserDefinedFunction. collectionLink [{}], udf id [{}]", collectionLink,
                        udf.getId());
                RxDocumentServiceRequest request = getUserDefinedFunctionRequest(collectionLink, udf, options,
                        OperationType.Upsert);
                return this.doUpsert(request).map(response -> toResourceResponse(response, UserDefinedFunction.class));

            } catch (Exception e) {
                // this is only in trace level to capture what's going on
                logger.debug("Failure in upserting a UserDefinedFunction due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });
    }

    @Override
    public Observable<ResourceResponse<UserDefinedFunction>> replaceUserDefinedFunction(UserDefinedFunction udf,
            RequestOptions options) {
        return Observable.defer(() -> {
            // we are using an observable factory here
            // observable will be created fresh upon subscription
            // this is to ensure we capture most up to date information (e.g.,
            // session)
            try {
                if (udf == null) {
                    throw new IllegalArgumentException("udf");
                }

                logger.debug("Replacing a UserDefinedFunction. udf id [{}]", udf.getId());
                validateResource(udf);

                String path = Utils.joinPath(udf.getSelfLink(), null);
                Map<String, String> requestHeaders = this.getRequestHeaders(options);
                RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.Replace,
                        ResourceType.UserDefinedFunction, path, udf, requestHeaders);
                return this.doReplace(request).map(response -> toResourceResponse(response, UserDefinedFunction.class));

            } catch (Exception e) {
                // this is only in trace level to capture what's going on
                logger.debug("Failure in replacing a UserDefinedFunction due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });
    }

    @Override
    public Observable<ResourceResponse<UserDefinedFunction>> deleteUserDefinedFunction(String udfLink,
            RequestOptions options) {
        return Observable.defer(() -> {
            // we are using an observable factory here
            // observable will be created fresh upon subscription
            // this is to ensure we capture most up to date information (e.g.,
            // session)
            try {
                if (StringUtils.isEmpty(udfLink)) {
                    throw new IllegalArgumentException("udfLink");
                }

                logger.debug("Deleting a UserDefinedFunction. udfLink [{}]", udfLink);
                String path = Utils.joinPath(udfLink, null);
                Map<String, String> requestHeaders = this.getRequestHeaders(options);
                RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.Delete,
                        ResourceType.UserDefinedFunction, path, requestHeaders);
                return this.doDelete(request).map(response -> toResourceResponse(response, UserDefinedFunction.class));

            } catch (Exception e) {
                // this is only in trace level to capture what's going on
                logger.debug("Failure in deleting a UserDefinedFunction due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });
    }

    @Override
    public Observable<ResourceResponse<UserDefinedFunction>> readUserDefinedFunction(String udfLink,
            RequestOptions options) {
        return Observable.defer(() -> {
            // we are using an observable factory here
            // observable will be created fresh upon subscription
            // this is to ensure we capture most up to date information (e.g.,
            // session)
            try {
                if (StringUtils.isEmpty(udfLink)) {
                    throw new IllegalArgumentException("udfLink");
                }

                logger.debug("Reading a UserDefinedFunction. udfLink [{}]", udfLink);
                String path = Utils.joinPath(udfLink, null);
                Map<String, String> requestHeaders = this.getRequestHeaders(options);
                RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.Read,
                        ResourceType.UserDefinedFunction, path, requestHeaders);

                return this.doRead(request).map(response -> toResourceResponse(response, UserDefinedFunction.class));

            } catch (Exception e) {
                // this is only in trace level to capture what's going on
                logger.debug("Failure in reading a UserDefinedFunction due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });
    }

    @Override
    public Observable<FeedResponsePage<UserDefinedFunction>> readUserDefinedFunctions(String collectionLink,
            FeedOptions options) {
        
        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }
        
        if (options == null)
            options = new FeedOptions();
        
        return getObservableFeedResponsePage(collectionLink, options, UserDefinedFunction.class,
                new Func4<String, String, Map<String, String>, RxDocumentClientImpl, Observable<FeedResponsePage<UserDefinedFunction>>>() {

                    @Override
                    public Observable<FeedResponsePage<UserDefinedFunction>> call(String token, String collectionLink,
                            Map<String, String> requestHeaders, RxDocumentClientImpl client) {
                        if (token != null)
                            requestHeaders.put(HttpConstants.HttpHeaders.CONTINUATION, token);

                        RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.ReadFeed,
                                ResourceType.UserDefinedFunction, Utils.joinPath(collectionLink, Paths.USER_DEFINED_FUNCTIONS_PATH_SEGMENT), requestHeaders);
                        try {
                            return client.doReadFeed(request).map(response -> toFeedResponsePage(response, UserDefinedFunction.class));
                        } catch (DocumentClientException e) {
                            return Observable.error(e);
                        }
                    }
                });
    }

    @Override
    public Observable<FeedResponsePage<UserDefinedFunction>> queryUserDefinedFunctions(String collectionLink,
            String query, FeedOptions options) {
        return this.rxWrapperClient.queryUserDefinedFunctions(collectionLink, query, options);
    }

    @Override
    public Observable<FeedResponsePage<UserDefinedFunction>> queryUserDefinedFunctions(String collectionLink,
            SqlQuerySpec querySpec, FeedOptions options) {
        return this.rxWrapperClient.queryUserDefinedFunctions(collectionLink, querySpec, options);
    }

    @Override
    public Observable<ResourceResponse<Attachment>> createAttachment(String documentLink, Attachment attachment,
            RequestOptions options) {

        return Observable.defer(() -> {
            try {
                logger.debug("Creating a Attachment. documentLink [{}], attachment id [{}]", documentLink,
                        attachment.getId());
                RxDocumentServiceRequest request = getAttachmentRequest(documentLink, attachment, options,
                        OperationType.Create);
                return this.doCreate(request).map(response -> toResourceResponse(response, Attachment.class));

            } catch (Exception e) {
                logger.debug("Failure in creating a Attachment due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });
    }

    @Override
    public Observable<ResourceResponse<Attachment>> upsertAttachment(String documentLink, Attachment attachment,
            RequestOptions options) {

        return Observable.defer(() -> {
            try {
                logger.debug("Upserting a Attachment. documentLink [{}], attachment id [{}]", documentLink,
                        attachment.getId());
                RxDocumentServiceRequest request = getAttachmentRequest(documentLink, attachment, options,
                        OperationType.Upsert);
                return this.doUpsert(request).map(response -> toResourceResponse(response, Attachment.class));

            } catch (Exception e) {
                logger.debug("Failure in upserting a Attachment due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });
    }

    @Override
    public Observable<ResourceResponse<Attachment>> replaceAttachment(Attachment attachment, RequestOptions options) {

        return Observable.defer(() -> {
            try {
                if (attachment == null) {
                    throw new IllegalArgumentException("attachment");
                }

                logger.debug("Replacing a Attachment. attachment id [{}]", attachment.getId());
                RxDocumentClientImpl.validateResource(attachment);

                String path = Utils.joinPath(attachment.getSelfLink(), null);
                Map<String, String> requestHeaders = getRequestHeaders(options);
                RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.Replace,
                        ResourceType.Attachment, path, attachment, requestHeaders);
                this.addPartitionKeyInformation(request, null, options);
                return this.doReplace(request).map(response -> toResourceResponse(response, Attachment.class));

            } catch (Exception e) {
                logger.debug("Failure in replacing a Attachment due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });
    }

    @Override
    public Observable<ResourceResponse<Attachment>> deleteAttachment(String attachmentLink, RequestOptions options) {

        return Observable.defer(() -> {
            try {
                if (StringUtils.isEmpty(attachmentLink)) {
                    throw new IllegalArgumentException("attachmentLink");
                }

                logger.debug("Deleting a Attachment. attachmentLink [{}]", attachmentLink);
                String path = Utils.joinPath(attachmentLink, null);
                Map<String, String> requestHeaders = getRequestHeaders(options);
                RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.Delete,
                        ResourceType.Attachment, path, requestHeaders);
                this.addPartitionKeyInformation(request, null, options);
                return this.doDelete(request).map(response -> toResourceResponse(response, Attachment.class));

            } catch (Exception e) {
                logger.debug("Failure in deleting a Attachment due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });
    }

    @Override
    public Observable<ResourceResponse<Attachment>> readAttachment(String attachmentLink, RequestOptions options) {

        return Observable.defer(() -> {
            try {
                if (StringUtils.isEmpty(attachmentLink)) {
                    throw new IllegalArgumentException("attachmentLink");
                }

                logger.debug("Reading a Attachment. attachmentLink [{}]", attachmentLink);
                String path = Utils.joinPath(attachmentLink, null);
                Map<String, String> requestHeaders = getRequestHeaders(options);
                RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.Read,
                        ResourceType.Attachment, path, requestHeaders);
                this.addPartitionKeyInformation(request, null, options);
                return this.doRead(request).map(response -> toResourceResponse(response, Attachment.class));

            } catch (Exception e) {
                logger.debug("Failure in reading a Attachment due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });
    }

    @Override
    public Observable<FeedResponsePage<Attachment>> readAttachments(String documentLink, FeedOptions options) {
        return this.rxWrapperClient.readAttachments(documentLink, options);
    }

    @Override
    public Observable<FeedResponsePage<Attachment>> queryAttachments(String documentLink, String query,
            FeedOptions options) {
        return this.rxWrapperClient.queryAttachments(documentLink, query, options);
    }

    @Override
    public Observable<FeedResponsePage<Attachment>> queryAttachments(String documentLink, SqlQuerySpec querySpec,
            FeedOptions options) {
        return this.rxWrapperClient.queryAttachments(documentLink, querySpec, options);
    }

    private RxDocumentServiceRequest getAttachmentRequest(String documentLink, Attachment attachment,
            RequestOptions options, OperationType operationType) {
        if (StringUtils.isEmpty(documentLink)) {
            throw new IllegalArgumentException("documentLink");
        }
        if (attachment == null) {
            throw new IllegalArgumentException("attachment");
        }

        RxDocumentClientImpl.validateResource(attachment);

        String path = Utils.joinPath(documentLink, Paths.ATTACHMENTS_PATH_SEGMENT);
        Map<String, String> requestHeaders = getRequestHeaders(options);
        RxDocumentServiceRequest request = RxDocumentServiceRequest.create(operationType, ResourceType.Attachment, path,
                attachment, requestHeaders);
        this.addPartitionKeyInformation(request, null, options);
        return request;
    }

    @Override
    public Observable<ResourceResponse<Attachment>> createAttachment(String documentLink, InputStream mediaStream,
            MediaOptions options) {

        return Observable.defer(() -> {
            try {
                logger.debug("Creating a Attachment. attachmentLink [{}]", documentLink);
                RxDocumentServiceRequest request = getAttachmentRequest(documentLink, mediaStream, options,
                        OperationType.Create);
                return this.doCreate(request).map(response -> toResourceResponse(response, Attachment.class));

            } catch (Exception e) {
                logger.debug("Failure in creating a Attachment due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });
    }

    @Override
    public Observable<ResourceResponse<Attachment>> upsertAttachment(String documentLink, InputStream mediaStream,
            MediaOptions options) {

        return Observable.defer(() -> {
            try {
                logger.debug("Upserting a Attachment. attachmentLink [{}]", documentLink);
                RxDocumentServiceRequest request = getAttachmentRequest(documentLink, mediaStream, options,
                        OperationType.Upsert);
                return this.doUpsert(request).map(response -> toResourceResponse(response, Attachment.class));

            } catch (Exception e) {
                logger.debug("Failure in upserting a Attachment due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });
    }

    private RxDocumentServiceRequest getAttachmentRequest(String documentLink, InputStream mediaStream,
            MediaOptions options, OperationType operationType) {
        if (StringUtils.isEmpty(documentLink)) {
            throw new IllegalArgumentException("documentLink");
        }
        if (mediaStream == null) {
            throw new IllegalArgumentException("mediaStream");
        }
        String path = Utils.joinPath(documentLink, Paths.ATTACHMENTS_PATH_SEGMENT);
        Map<String, String> requestHeaders = this.getMediaHeaders(options);
        RxDocumentServiceRequest request = RxDocumentServiceRequest.create(operationType, ResourceType.Attachment, path,
                mediaStream, requestHeaders);
        request.setIsMedia(true);
        this.addPartitionKeyInformation(request, null, null);
        return request;
    }

    @Override
    public Observable<MediaResponse> readMedia(String mediaLink) {
        return this.rxWrapperClient.readMedia(mediaLink);
    }

    @Override
    public Observable<MediaResponse> updateMedia(String mediaLink, InputStream mediaStream, MediaOptions options) {
        return this.rxWrapperClient.updateMedia(mediaLink, mediaStream, options);
    }

    @Override
    public Observable<ResourceResponse<Conflict>> readConflict(String conflictLink, RequestOptions options) {

        return Observable.defer(() -> {
            try {
                if (StringUtils.isEmpty(conflictLink)) {
                    throw new IllegalArgumentException("conflictLink");
                }

                logger.debug("Reading a Conflict. conflictLink [{}]", conflictLink);
                String path = Utils.joinPath(conflictLink, null);
                Map<String, String> requestHeaders = getRequestHeaders(options);
                RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.Read,
                        ResourceType.Conflict, path, requestHeaders);
                this.addPartitionKeyInformation(request, null, options);
                return this.doRead(request).map(response -> toResourceResponse(response, Conflict.class));

            } catch (Exception e) {
                logger.debug("Failure in reading a Conflict due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });
    }

    @Override
    public Observable<FeedResponsePage<Conflict>> readConflicts(String collectionLink, FeedOptions options) {
        
        if (StringUtils.isEmpty(collectionLink)) {
            throw new IllegalArgumentException("collectionLink");
        }

        if (options == null)
            options = new FeedOptions();
        
        return getObservableFeedResponsePage(collectionLink, options, Conflict.class,
                new Func4<String, String, Map<String, String>, RxDocumentClientImpl, Observable<FeedResponsePage<Conflict>>>() {

                    @Override
                    public Observable<FeedResponsePage<Conflict>> call(String token, String collectionLink,
                            Map<String, String> requestHeaders, RxDocumentClientImpl client) {
                        if (token != null)
                            requestHeaders.put(HttpConstants.HttpHeaders.CONTINUATION, token);

                        RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.ReadFeed,
                                ResourceType.Conflict, Utils.joinPath(collectionLink, Paths.CONFLICTS_PATH_SEGMENT), requestHeaders);
                        try {
                            return client.doReadFeed(request).map(response -> toFeedResponsePage(response, Conflict.class));
                        } catch (DocumentClientException e) {
                            return Observable.error(e);
                        }
                    }
                });
    }

    @Override
    public Observable<FeedResponsePage<Conflict>> queryConflicts(String collectionLink, String query,
            FeedOptions options) {
        return this.rxWrapperClient.queryConflicts(collectionLink, query, options);
    }

    @Override
    public Observable<FeedResponsePage<Conflict>> queryConflicts(String collectionLink, SqlQuerySpec querySpec,
            FeedOptions options) {
        return this.rxWrapperClient.queryConflicts(collectionLink, querySpec, options);
    }

    @Override
    public Observable<ResourceResponse<Conflict>> deleteConflict(String conflictLink, RequestOptions options) {

        return Observable.defer(() -> {
            try {
                if (StringUtils.isEmpty(conflictLink)) {
                    throw new IllegalArgumentException("conflictLink");
                }

                logger.debug("Deleting a Conflict. conflictLink [{}]", conflictLink);
                String path = Utils.joinPath(conflictLink, null);
                Map<String, String> requestHeaders = getRequestHeaders(options);
                RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.Read,
                        ResourceType.Conflict, path, requestHeaders);
                this.addPartitionKeyInformation(request, null, options);
                return this.doDelete(request).map(response -> toResourceResponse(response, Conflict.class));

            } catch (Exception e) {
                logger.debug("Failure in deleting a Conflict due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });
    }

    @Override
    public Observable<ResourceResponse<User>> createUser(String databaseLink, User user, RequestOptions options) {

        return Observable.defer(() -> {
            try {
                logger.debug("Creating a User. databaseLink [{}], user id [{}]", databaseLink, user.getId());
                RxDocumentServiceRequest request = getUserRequest(databaseLink, user, options, OperationType.Create);
                return this.doCreate(request).map(response -> toResourceResponse(response, User.class));

            } catch (Exception e) {
                logger.debug("Failure in creating a User due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });

    }

    @Override
    public Observable<ResourceResponse<User>> upsertUser(String databaseLink, User user, RequestOptions options) {

        return Observable.defer(() -> {
            try {
                logger.debug("Upserting a User. databaseLink [{}], user id [{}]", databaseLink, user.getId());
                RxDocumentServiceRequest request = getUserRequest(databaseLink, user, options, OperationType.Upsert);
                return this.doUpsert(request).map(response -> toResourceResponse(response, User.class));

            } catch (Exception e) {
                logger.debug("Failure in upserting a User due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });
    }

    private RxDocumentServiceRequest getUserRequest(String databaseLink, User user, RequestOptions options,
            OperationType operationType) {
        if (StringUtils.isEmpty(databaseLink)) {
            throw new IllegalArgumentException("databaseLink");
        }
        if (user == null) {
            throw new IllegalArgumentException("user");
        }

        RxDocumentClientImpl.validateResource(user);

        String path = Utils.joinPath(databaseLink, Paths.USERS_PATH_SEGMENT);
        Map<String, String> requestHeaders = getRequestHeaders(options);
        RxDocumentServiceRequest request = RxDocumentServiceRequest.create(operationType, ResourceType.User, path, user,
                requestHeaders);
        return request;
    }

    @Override
    public Observable<ResourceResponse<User>> replaceUser(User user, RequestOptions options) {

        return Observable.defer(() -> {
            try {
                if (user == null) {
                    throw new IllegalArgumentException("user");
                }
                logger.debug("Replacing a User. user id [{}]", user.getId());
                RxDocumentClientImpl.validateResource(user);

                String path = Utils.joinPath(user.getSelfLink(), null);
                Map<String, String> requestHeaders = getRequestHeaders(options);
                RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.Replace,
                        ResourceType.User, path, user, requestHeaders);
                return this.doReplace(request).map(response -> toResourceResponse(response, User.class));

            } catch (Exception e) {
                logger.debug("Failure in replacing a User due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });
    }

    @Override
    public Observable<ResourceResponse<User>> deleteUser(String userLink, RequestOptions options) {

        return Observable.defer(() -> {
            try {
                if (StringUtils.isEmpty(userLink)) {
                    throw new IllegalArgumentException("userLink");
                }
                logger.debug("Deleting a User. userLink [{}]", userLink);
                String path = Utils.joinPath(userLink, null);
                Map<String, String> requestHeaders = getRequestHeaders(options);
                RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.Delete,
                        ResourceType.User, path, requestHeaders);
                return this.doDelete(request).map(response -> toResourceResponse(response, User.class));

            } catch (Exception e) {
                logger.debug("Failure in deleting a User due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });
    }

    @Override
    public Observable<ResourceResponse<User>> readUser(String userLink, RequestOptions options) {

        return Observable.defer(() -> {
            try {
                if (StringUtils.isEmpty(userLink)) {
                    throw new IllegalArgumentException("userLink");
                }
                logger.debug("Reading a User. userLink [{}]", userLink);
                String path = Utils.joinPath(userLink, null);
                Map<String, String> requestHeaders = getRequestHeaders(options);
                RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.Read,
                        ResourceType.User, path, requestHeaders);
                return this.doRead(request).map(response -> toResourceResponse(response, User.class));

            } catch (Exception e) {
                logger.debug("Failure in reading a User due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });
    }

    @Override
    public Observable<FeedResponsePage<User>> readUsers(String databaseLink, FeedOptions options) {
        
        if (StringUtils.isEmpty(databaseLink)) {
            throw new IllegalArgumentException("databaseLink");
        }
        
        if (options == null)
            options = new FeedOptions();
        
        return getObservableFeedResponsePage(databaseLink, options, User.class,
                new Func4<String, String, Map<String, String>, RxDocumentClientImpl, Observable<FeedResponsePage<User>>>() {

                    @Override
                    public Observable<FeedResponsePage<User>> call(String token, String databaseLink,
                            Map<String, String> requestHeaders, RxDocumentClientImpl client) {
                        if (token != null)
                            requestHeaders.put(HttpConstants.HttpHeaders.CONTINUATION, token);

                        RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.ReadFeed,
                                ResourceType.User, Utils.joinPath(databaseLink, Paths.USERS_PATH_SEGMENT), requestHeaders);
                        try {
                            return client.doReadFeed(request).map(response -> toFeedResponsePage(response, User.class));
                        } catch (DocumentClientException e) {
                            return Observable.error(e);
                        }
                    }
                });
    }

    @Override
    public Observable<FeedResponsePage<User>> queryUsers(String databaseLink, String query, FeedOptions options) {
        return this.rxWrapperClient.queryUsers(databaseLink, query, options);
    }

    @Override
    public Observable<FeedResponsePage<User>> queryUsers(String databaseLink, SqlQuerySpec querySpec,
            FeedOptions options) {
        return this.rxWrapperClient.queryUsers(databaseLink, querySpec, options);
    }

    @Override
    public Observable<ResourceResponse<Permission>> createPermission(String userLink, Permission permission,
            RequestOptions options) {

        return Observable.defer(() -> {
            try {
                logger.debug("Creating a Permission. userLink [{}], permission id [{}]", userLink, permission.getId());
                RxDocumentServiceRequest request = getPermissionRequest(userLink, permission, options,
                        OperationType.Create);
                return this.doCreate(request).map(response -> toResourceResponse(response, Permission.class));

            } catch (Exception e) {
                logger.debug("Failure in creating a Permission due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });
    }

    @Override
    public Observable<ResourceResponse<Permission>> upsertPermission(String userLink, Permission permission,
            RequestOptions options) {

        return Observable.defer(() -> {
            try {
                logger.debug("Upserting a Permission. userLink [{}], permission id [{}]", userLink, permission.getId());
                RxDocumentServiceRequest request = getPermissionRequest(userLink, permission, options,
                        OperationType.Upsert);
                return this.doUpsert(request).map(response -> toResourceResponse(response, Permission.class));

            } catch (Exception e) {
                logger.debug("Failure in upserting a Permission due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });
    }

    private RxDocumentServiceRequest getPermissionRequest(String userLink, Permission permission,
            RequestOptions options, OperationType operationType) {
        if (StringUtils.isEmpty(userLink)) {
            throw new IllegalArgumentException("userLink");
        }
        if (permission == null) {
            throw new IllegalArgumentException("permission");
        }

        RxDocumentClientImpl.validateResource(permission);

        String path = Utils.joinPath(userLink, Paths.PERMISSIONS_PATH_SEGMENT);
        Map<String, String> requestHeaders = getRequestHeaders(options);
        RxDocumentServiceRequest request = RxDocumentServiceRequest.create(operationType, ResourceType.Permission, path,
                permission, requestHeaders);
        return request;
    }

    @Override
    public Observable<ResourceResponse<Permission>> replacePermission(Permission permission, RequestOptions options) {

        return Observable.defer(() -> {
            try {
                if (permission == null) {
                    throw new IllegalArgumentException("permission");
                }
                logger.debug("Replacing a Permission. permission id [{}]", permission.getId());
                RxDocumentClientImpl.validateResource(permission);

                String path = Utils.joinPath(permission.getSelfLink(), null);
                Map<String, String> requestHeaders = getRequestHeaders(options);
                RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.Replace,
                        ResourceType.Permission, path, permission, requestHeaders);
                return this.doReplace(request).map(response -> toResourceResponse(response, Permission.class));

            } catch (Exception e) {
                logger.debug("Failure in replacing a Permission due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });
    }

    @Override
    public Observable<ResourceResponse<Permission>> deletePermission(String permissionLink, RequestOptions options) {

        return Observable.defer(() -> {
            try {
                if (StringUtils.isEmpty(permissionLink)) {
                    throw new IllegalArgumentException("permissionLink");
                }
                logger.debug("Deleting a Permission. permissionLink [{}]", permissionLink);
                String path = Utils.joinPath(permissionLink, null);
                Map<String, String> requestHeaders = getRequestHeaders(options);
                RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.Delete,
                        ResourceType.Permission, path, requestHeaders);
                return this.doDelete(request).map(response -> toResourceResponse(response, Permission.class));

            } catch (Exception e) {
                logger.debug("Failure in deleting a Permission due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });
    }

    @Override
    public Observable<ResourceResponse<Permission>> readPermission(String permissionLink, RequestOptions options) {

        return Observable.defer(() -> {
            try {
                if (StringUtils.isEmpty(permissionLink)) {
                    throw new IllegalArgumentException("permissionLink");
                }
                logger.debug("Reading a Permission. permissionLink [{}]", permissionLink);
                String path = Utils.joinPath(permissionLink, null);
                Map<String, String> requestHeaders = getRequestHeaders(options);
                RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.Read,
                        ResourceType.Permission, path, requestHeaders);
                return this.doRead(request).map(response -> toResourceResponse(response, Permission.class));

            } catch (Exception e) {
                logger.debug("Failure in reading a Permission due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });
    }

    @Override
    public Observable<FeedResponsePage<Permission>> readPermissions(String userLink, FeedOptions options) {
        
        if (StringUtils.isEmpty(userLink)) {
            throw new IllegalArgumentException("userLink");
        }
        
        if (options == null)
            options = new FeedOptions();
        
        return getObservableFeedResponsePage(userLink, options, Permission.class,
                new Func4<String, String, Map<String, String>, RxDocumentClientImpl, Observable<FeedResponsePage<Permission>>>() {

                    @Override
                    public Observable<FeedResponsePage<Permission>> call(String token, String userLink,
                            Map<String, String> requestHeaders, RxDocumentClientImpl client) {
                        if (token != null)
                            requestHeaders.put(HttpConstants.HttpHeaders.CONTINUATION, token);

                        RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.ReadFeed,
                                ResourceType.Permission, Utils.joinPath(userLink, Paths.PERMISSIONS_PATH_SEGMENT), requestHeaders);
                        
                        try {
                            return client.doReadFeed(request)
                                    .map(response -> toFeedResponsePage(response, Permission.class));
                        } catch (DocumentClientException e) {
                            return Observable.error(e);
                        }
                    }
                });
    }

    @Override
    public Observable<FeedResponsePage<Permission>> queryPermissions(String permissionLink, String query,
            FeedOptions options) {
        return this.rxWrapperClient.queryPermissions(permissionLink, query, options);
    }

    @Override
    public Observable<FeedResponsePage<Permission>> queryPermissions(String permissionLink, SqlQuerySpec querySpec,
            FeedOptions options) {
        return this.rxWrapperClient.queryPermissions(permissionLink, querySpec, options);
    }

    @Override
    public Observable<ResourceResponse<Offer>> replaceOffer(Offer offer) {

        return Observable.defer(() -> {
            try {
                if (offer == null) {
                    throw new IllegalArgumentException("offer");
                }
                logger.debug("Replacing an Offer. offer id [{}]", offer.getId());
                RxDocumentClientImpl.validateResource(offer);

                String path = Utils.joinPath(offer.getSelfLink(), null);
                RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.Replace,
                        ResourceType.Offer, path, offer, null);
                return this.doReplace(request).map(response -> toResourceResponse(response, Offer.class));

            } catch (Exception e) {
                logger.debug("Failure in replacing an Offer due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });
    }

    @Override
    public Observable<ResourceResponse<Offer>> readOffer(String offerLink) {

        return Observable.defer(() -> {
            try {
                if (StringUtils.isEmpty(offerLink)) {
                    throw new IllegalArgumentException("offerLink");
                }
                logger.debug("Reading an Offer. offerLink [{}]", offerLink);
                String path = Utils.joinPath(offerLink, null);
                RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.Read,
                        ResourceType.Offer, path, null);
                return this.doRead(request).map(response -> toResourceResponse(response, Offer.class));

            } catch (Exception e) {
                logger.debug("Failure in reading an Offer due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });
    }

    @Override
    public Observable<FeedResponsePage<Offer>> readOffers(FeedOptions options) {
        
        if (options == null)
            options = new FeedOptions();
        
        return getObservableFeedResponsePage(null, options, Offer.class,
                new Func4<String, String, Map<String, String>, RxDocumentClientImpl, Observable<FeedResponsePage<Offer>>>() {

                    @Override
                    public Observable<FeedResponsePage<Offer>> call(String token, String resourceLink,
                            Map<String, String> requestHeaders, RxDocumentClientImpl client) {
                        if (token != null)
                            requestHeaders.put(HttpConstants.HttpHeaders.CONTINUATION, token);

                        RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.ReadFeed,
                                ResourceType.Offer, Utils.joinPath(Paths.OFFERS_PATH_SEGMENT, null), requestHeaders);
                        try {
                            return client.doReadFeed(request).map(response -> toFeedResponsePage(response, Offer.class));
                        } catch (DocumentClientException e) {
                            return Observable.error(e);
                        }
                    }
                });
    }

    @Override
    public Observable<FeedResponsePage<Offer>> queryOffers(String query, FeedOptions options) {
        return this.rxWrapperClient.queryOffers(query, options);
    }

    @Override
    public Observable<FeedResponsePage<Offer>> queryOffers(SqlQuerySpec querySpec, FeedOptions options) {
        return this.rxWrapperClient.queryOffers(querySpec, options);
    }

    @Override
    public Observable<DatabaseAccount> getDatabaseAccount() {

        return Observable.defer(() -> {
            try {
                logger.debug("Getting Database Account");
                RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.Read,
                        ResourceType.DatabaseAccount, "", // path
                        null);
                return this.doRead(request).map(response -> toDatabaseAccount(response));

            } catch (Exception e) {
                logger.debug("Failure in getting Database Account due to [{}]", e.getMessage(), e);
                return Observable.error(e);
            }
        });
    }

    public Observable<DatabaseAccount> getDatabaseAccountFromEndpoint(URI endpoint) {
        return Observable.defer(() -> {
            RxDocumentServiceRequest request = RxDocumentServiceRequest.create(OperationType.Read,
                    ResourceType.DatabaseAccount, "", null);
            this.putMoreContentIntoDocumentServiceRequest(request, HttpConstants.HttpMethods.GET);

            request.setEndpointOverride(endpoint);
            return this.gatewayProxy.doRead(request).doOnError(e -> {
                String message = "Failed to retrieve database account information. %s";
                Throwable cause = e.getCause();
                if (cause != null) {
                    message = String.format(message, cause.toString());
                } else {
                    message = String.format(message, e.toString());
                }
                logger.warn(message);
            }).map(rsp -> rsp.getResource(DatabaseAccount.class));
        });
    }

    private void safeShutdownExecutorService(ExecutorService exS) {
        if (exS == null) {
            return;
        }

        try {
            exS.shutdown();
            exS.awaitTermination(15, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.warn("Failure in shutting down a executor service", e);
        }
    }

    @Override
    public void close() {

        this.safeShutdownExecutorService(this.collectionCacheExecutorService);
        this.safeShutdownExecutorService(this.computationExecutor);

        try {
            this.rxWrapperClient.close();
        } catch (Exception e) {
            logger.warn("Failure in shutting down rxWrapperClient", e);
        }

        try {
            this.rxClient.shutdown();
        } catch (Exception e) {
            logger.warn("Failure in shutting down rxClient", e);
        }
    }

    private Func1<Observable<? extends Throwable>, Observable<Long>> createExecuteRequestRetryHandler(
            RxDocumentServiceRequest request) {
        return RetryFunctionFactory
                .from(new ExecuteDocumentClientRequestRetryHandler(request, globalEndpointManager, this));
    }

}
