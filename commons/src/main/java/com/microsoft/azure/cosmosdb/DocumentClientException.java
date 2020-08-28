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

package com.microsoft.azure.cosmosdb;

import com.microsoft.azure.cosmosdb.internal.Constants;
import com.microsoft.azure.cosmosdb.internal.HttpConstants;
import com.microsoft.azure.cosmosdb.internal.directconnectivity.Uri;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class defines a custom exception type for all operations on
 * DocumentClient in the Azure Cosmos DB database service. Applications are expected to catch DocumentClientException
 * and handle errors as appropriate when calling methods on DocumentClient.
 * <p>
 * Errors coming from the service during normal execution are converted to
 * DocumentClientException before returning to the application with the following exception:
 * <p>
 * When a BE error is encountered during a QueryIterable&lt;T&gt; iteration, an IllegalStateException
 * is thrown instead of DocumentClientException.
 * <p>
 * When a transport level error happens that request is not able to reach the service,
 * an IllegalStateException is thrown instead of DocumentClientException.
 */
public class DocumentClientException extends Exception {

    private static final long serialVersionUID = 1L;

    private final Map<String, String> requestHeaders;
    private final int statusCode;
    private Map<String, String> responseHeaders;

    private volatile ClientSideRequestStatistics clientSideRequestStatistics;
    private volatile Error error;
    private volatile long lsn;
    private volatile String partitionKeyRangeId;
    private volatile Uri requestUri;
    private volatile String resourceAddress;

    /**
     * Creates a new instance of the {@link DocumentClientException} class.
     *
     * @param statusCode the http status code of the response.
     */
    public DocumentClientException(int statusCode) {
        this(statusCode, null, null, null);
    }

    /**
     * Creates a new instance of the {@link DocumentClientException} class.
     *
     * @param statusCode   the http status code of the response.
     * @param errorMessage the error message.
     */
    public DocumentClientException(int statusCode, String errorMessage) {
        this(statusCode, errorMessage, null, null);
        this.error = new Error();
        error.set(Constants.Properties.MESSAGE, errorMessage);
    }

    /**
     * Creates a new instance of the {@link DocumentClientException} class.
     *
     * @param statusCode     the http status code of the response.
     * @param innerException the original exception.
     */
    public DocumentClientException(int statusCode, Exception innerException) {
        this(statusCode, null, null, innerException);
    }

    /**
     * Creates a new instance of the {@link DocumentClientException} class.
     *
     * @param statusCode      the http status code of the response.
     * @param errorResource   the error resource object.
     * @param responseHeaders the response headers.
     */
    public DocumentClientException(int statusCode, Error errorResource, Map<String, String> responseHeaders) {
        this(/* resourceAddress */ null, statusCode, errorResource, responseHeaders);
    }

    /**
     * Creates a new instance of the {@link DocumentClientException} class.
     *
     * @param resourceAddress the address of the resource the request is associated with.
     * @param statusCode      the http status code of the response.
     * @param errorResource   the error resource object.
     * @param responseHeaders the response headers.
     */

    public DocumentClientException(
        String resourceAddress, int statusCode, Error errorResource, Map<String, String> responseHeaders) {
        this(statusCode, errorResource == null ? null : errorResource.getMessage(), responseHeaders, null);
        this.resourceAddress = resourceAddress;
        this.error = errorResource;
    }

    /**
     * Creates a new instance of the {@link DocumentClientException} class.
     *
     * @param message         the string message.
     * @param statusCode      the http status code of the response.
     * @param exception       the exception object.
     * @param responseHeaders the response headers.
     * @param resourceAddress the address of the resource the request is associated with.
     */
    public DocumentClientException(
        String message,
        Exception exception,
        Map<String, String> responseHeaders,
        int statusCode,
        String resourceAddress) {
        this(statusCode, message, responseHeaders, exception);
        this.resourceAddress = resourceAddress;
    }

    private DocumentClientException(
        int statusCode, String message, Map<String, String> responseHeaders, Throwable cause) {
        super(message, cause);
        this.requestHeaders = new ConcurrentHashMap<>();
        this.responseHeaders = responseHeaders == null
            ? new ConcurrentHashMap<>()
            : new ConcurrentHashMap<>(responseHeaders);
        this.statusCode = statusCode;
    }

    /**
     * Gets the activity ID associated with the request.
     *
     * @return the activity ID.
     */
    public String getActivityId() {
        if (this.responseHeaders != null) {
            return this.responseHeaders.get(HttpConstants.HttpHeaders.ACTIVITY_ID);
        }

        return null;
    }

    /**
     * Gets the Client side request statistics associated with this exception.
     *
     * @return Client side request statistics associated with this exception.
     */
    public ClientSideRequestStatistics getClientSideRequestStatistics() {
        return clientSideRequestStatistics;
    }

    /**
     * Sets the Client side request statistics associated with this exception.
     */
    // TODO (DANOBLE) Consider changing the access of this method to package private and adding it to BridgeInternal
    public void setClientSideRequestStatistics(ClientSideRequestStatistics clientSideRequestStatistics) {
        this.clientSideRequestStatistics = clientSideRequestStatistics;
    }

    /**
     * Gets the error code associated with the exception.
     *
     * @return the error.
     */
    public Error getError() {
        return this.error;
    }

    /**
     * Gets the LSN associated with this {@link DocumentClientException exception}.
     *
     * @return the LSN associated with this {@link DocumentClientException exception}.
     */
    public long getLsn() {
        return lsn;
    }

    void setLsn(long lsn) {
        this.lsn = lsn;
    }

    @Override
    public String getMessage() {
        if (clientSideRequestStatistics == null) {
            return getInnerErrorMessage();
        }
        return getInnerErrorMessage() + ", " + clientSideRequestStatistics.toString();
    }

    public String getPartitionKeyRangeId() {
        return this.partitionKeyRangeId;
    }

    void setPartitionKeyRangeId(String partitionKeyRangeId) {
        this.partitionKeyRangeId = partitionKeyRangeId;
    }

    public Map<String, String> getRequestHeaders() {
        return this.requestHeaders;
    }

    void setRequestHeaders(Map<String, String> values) {

        this.requestHeaders.clear();

        if (values != null) {
            for (Map.Entry<String, String> entry : values.entrySet()) {
                final String key = entry.getKey();
                final String value = entry.getValue();
                if (key != null && value != null) {
                    this.requestHeaders.put(key, value);
                }
            }
        }
    }

    public Uri getRequestUri() {
        return requestUri;
    }

    public void setRequestUri(Uri requestUri) {
        this.requestUri = requestUri;
    }

    /**
     * Gets the response headers as key-value pairs
     *
     * @return the response headers
     */
    public Map<String, String> getResponseHeaders() {
        return this.responseHeaders;
    }

    /**
     * Gets the recommended time interval after which the client can retry
     * failed requests
     *
     * @return the recommended time interval after which the client can retry
     * failed requests.
     */
    public long getRetryAfterInMilliseconds() {
        long retryIntervalInMilliseconds = 0;

        if (this.responseHeaders != null) {
            String header = this.responseHeaders.get(HttpConstants.HttpHeaders.RETRY_AFTER_IN_MILLISECONDS);

            if (StringUtils.isNotEmpty(header)) {
                try {
                    retryIntervalInMilliseconds = Long.parseLong(header);
                } catch (NumberFormatException e) {
                    // If the value cannot be parsed as long, return 0.
                }
            }
        }

        // In the absence of explicit guidance from the backend, don't introduce any unilateral retry delays here.
        return retryIntervalInMilliseconds;
    }

    /**
     * Gets the http status code.
     *
     * @return the status code.
     */
    public int getStatusCode() {
        return this.statusCode;
    }

    /**
     * Gets the sub status code.
     *
     * @return the status code.
     */
    public int getSubStatusCode() {
        int code = HttpConstants.SubStatusCodes.UNKNOWN;
        if (this.responseHeaders != null) {
            String subStatusString = this.responseHeaders.get(HttpConstants.HttpHeaders.SUB_STATUS);
            if (StringUtils.isNotEmpty(subStatusString)) {
                try {
                    code = Integer.parseInt(subStatusString);
                } catch (NumberFormatException e) {
                    // If value cannot be parsed as Integer, return Unknown.
                }
            }
        }

        return code;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                "error=" + error +
                ", resourceAddress='" + resourceAddress + '\'' +
                ", statusCode=" + statusCode +
                ", message=" + getMessage() +
                ", getCauseInfo=" + getCauseInfo() +
                ", responseHeaders=" + responseHeaders +
                ", requestHeaders=" + requestHeaders +
                '}';
    }

	void setSubStatusCode(int subStatusCode) {
		this.responseHeaders.put(HttpConstants.HttpHeaders.SUB_STATUS, Integer.toString(subStatusCode));
	}

    String getInnerErrorMessage() {
        String innerErrorMessage = super.getMessage();
        if (error != null) {
            innerErrorMessage = error.getMessage();
            if (innerErrorMessage == null) {
                innerErrorMessage = String.valueOf(error.get("Errors"));
            }
        }
        return innerErrorMessage;
    }

    /**
     * Gets the resource address associated with this exception.
     *
     * @return the resource address associated with this exception.
     */
    String getResourceAddress() {
        return this.resourceAddress;
    }

    void setResourceAddress(String resourceAddress) {
        this.resourceAddress = resourceAddress;
    }

    private String getCauseInfo() {
        Throwable cause = getCause();
        if (cause != null) {
            return String.format("[class: %s, message: %s]", cause.getClass(), cause.getMessage());
        }
        return null;
    }
}
