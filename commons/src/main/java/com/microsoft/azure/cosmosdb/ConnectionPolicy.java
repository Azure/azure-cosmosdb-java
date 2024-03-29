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

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Represents the Connection policy associated with a DocumentClient in the Azure Cosmos DB database service.
 */
public final class ConnectionPolicy {

    private static final int DEFAULT_REQUEST_TIMEOUT_IN_MILLIS = 60 * 1000;
    // defaultMediaRequestTimeout is based upon the blob client timeout and the
    // retry policy.
    private static final int DEFAULT_MEDIA_REQUEST_TIMEOUT_IN_MILLIS = 300 * 1000;
    private static final int DEFAULT_IDLE_CONNECTION_TIMEOUT_IN_MILLIS = 60 * 1000;

    private static final int DEFAULT_MAX_POOL_SIZE = 1000;

    private static ConnectionPolicy default_policy = null;
    private int requestTimeoutInMillis;
    private int mediaRequestTimeoutInMillis;
    private ConnectionMode connectionMode;
    private MediaReadMode mediaReadMode;
    private int maxPoolSize;
    private int idleConnectionTimeoutInMillis;
    private String userAgentSuffix;
    private RetryOptions retryOptions;
    private boolean enableEndpointDiscovery = true;
    private List<String> preferredLocations;
    private boolean usingMultipleWriteLocations;
    private InetSocketAddress inetSocketProxyAddress;
    private Boolean enableReadRequestsFallback;

    /**
     * Constructor.
     */
    public ConnectionPolicy() {
        this.connectionMode = ConnectionMode.Gateway;
        this.enableReadRequestsFallback = null;
        this.idleConnectionTimeoutInMillis = DEFAULT_IDLE_CONNECTION_TIMEOUT_IN_MILLIS;
        this.maxPoolSize = DEFAULT_MAX_POOL_SIZE;
        this.mediaReadMode = MediaReadMode.Buffered;
        this.mediaRequestTimeoutInMillis = ConnectionPolicy.DEFAULT_MEDIA_REQUEST_TIMEOUT_IN_MILLIS;
        this.requestTimeoutInMillis = ConnectionPolicy.DEFAULT_REQUEST_TIMEOUT_IN_MILLIS;
        this.retryOptions = new RetryOptions();
        this.userAgentSuffix = "";
    }

    /**
     * Gets the default connection policy.
     *
     * @return the default connection policy.
     */
    public static ConnectionPolicy GetDefault() {
        if (ConnectionPolicy.default_policy == null) {
            ConnectionPolicy.default_policy = new ConnectionPolicy();
        }
        return ConnectionPolicy.default_policy;
    }

    /**
     * Gets the request timeout (time to wait for response from network peer) in
     * milliseconds. 
     *
     * @return the request timeout in milliseconds.
     */
    public int getRequestTimeoutInMillis() {
        return this.requestTimeoutInMillis;
    }

    /**
     * Sets the request timeout (time to wait for response from network peer) in
     * milliseconds. The default is 60 seconds.
     *
     * @param requestTimeoutInMillis the request timeout in milliseconds.
     */
    public void setRequestTimeoutInMillis(int requestTimeoutInMillis) {
        this.requestTimeoutInMillis = requestTimeoutInMillis;
    }

    /**
     * Gets or sets time to wait for response from network peer for attachment
     * content (aka media) operations.
     *
     * @return the media request timeout in milliseconds.
     */
    public int getMediaRequestTimeoutInMillis() {
        return this.mediaRequestTimeoutInMillis;
    }

    /**
     * Gets or sets Time to wait for response from network peer for attachment
     * content (aka media) operations.
     *
     * @param mediaRequestTimeoutInMillis the media request timeout in milliseconds.
     */
    public void setMediaRequestTimeoutInMillis(int mediaRequestTimeoutInMillis) {
        this.mediaRequestTimeoutInMillis = mediaRequestTimeoutInMillis;
    }

    /**
     * Gets the connection mode used in the client.
     *
     * @return the connection mode.
     */
    public ConnectionMode getConnectionMode() {
        return this.connectionMode;
    }

    /**
     * Sets the connection mode used in the client.
     *
     * @param connectionMode the connection mode.
     */
    public void setConnectionMode(ConnectionMode connectionMode) {
        this.connectionMode = connectionMode;
    }

    /**
     * Gets the attachment content (aka media) download mode.
     *
     * @return the media read mode.
     */
    public MediaReadMode getMediaReadMode() {
        return this.mediaReadMode;
    }

    /**
     * Sets the attachment content (aka media) download mode.
     *
     * @param mediaReadMode the media read mode.
     */
    public void setMediaReadMode(MediaReadMode mediaReadMode) {
        this.mediaReadMode = mediaReadMode;
    }

    /**
     * Gets the value of the connection pool size the client is using.
     *
     * @return connection pool size.
     */
    public int getMaxPoolSize() {
        return this.maxPoolSize;
    }

    /**
     * Sets the value of the connection pool size, the default
     * is 1000.
     *
     * @param maxPoolSize The value of the connection pool size.
     */
    public void setMaxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }

    /**
     * Gets the value of the timeout for an idle connection, the default is 60
     * seconds.
     *
     * @return Idle connection timeout.
     */
    public int getIdleConnectionTimeoutInMillis() {
        return this.idleConnectionTimeoutInMillis;
    }

    /**
     * sets the value of the timeout for an idle connection. After that time,
     * the connection will be automatically closed.
     *
     * @param idleConnectionTimeoutInMillis the timeout for an idle connection in seconds.
     */
    public void setIdleConnectionTimeoutInMillis(int idleConnectionTimeoutInMillis) {
        this.idleConnectionTimeoutInMillis = idleConnectionTimeoutInMillis;
    }

    /**
     * Gets the value of user-agent suffix.
     *
     * @return the value of user-agent suffix.
     */
    public String getUserAgentSuffix() {
        return this.userAgentSuffix;
    }

    /**
     * sets the value of the user-agent suffix.
     *
     * @param userAgentSuffix The value to be appended to the user-agent header, this is
     *                        used for monitoring purposes.
     */
    public void setUserAgentSuffix(String userAgentSuffix) {
        this.userAgentSuffix = userAgentSuffix;
    }

    /**
     * Gets the retry policy options associated with the DocumentClient instance.
     *
     * @return the RetryOptions instance.
     */
    public RetryOptions getRetryOptions() {
        return this.retryOptions;
    }

    /**
     * Sets the retry policy options associated with the DocumentClient instance.
     * <p>
     * Properties in the RetryOptions class allow application to customize the built-in
     * retry policies. This property is optional. When it's not set, the SDK uses the
     * default values for configuring the retry policies.  See RetryOptions class for
     * more details.
     *
     * @param retryOptions the RetryOptions instance.
     */
    public void setRetryOptions(RetryOptions retryOptions) {
        if (retryOptions == null) {
            throw new IllegalArgumentException("retryOptions value must not be null.");
        }

        this.retryOptions = retryOptions;
    }

    /**
     * Gets the flag to enable endpoint discovery for geo-replicated database accounts.
     *
     * @return whether endpoint discovery is enabled.
     */
    public boolean getEnableEndpointDiscovery() {
        return this.enableEndpointDiscovery;
    }

    /**
     * Sets the flag to enable endpoint discovery for geo-replicated database accounts.
     * <p>
     * When EnableEndpointDiscovery is true, the SDK will automatically discover the
     * current write and read regions to ensure requests are sent to the correct region
     * based on the capability of the region and the user's preference.
     * <p>
     * The default value for this property is true indicating endpoint discovery is enabled.
     *
     * @param enableEndpointDiscovery true if EndpointDiscovery is enabled.
     */
    public void setEnableEndpointDiscovery(boolean enableEndpointDiscovery) {
        this.enableEndpointDiscovery = enableEndpointDiscovery;
    }

    /**
     * Gets the flag to enable writes on any locations (regions) for geo-replicated database accounts in the Azure Cosmos DB service.
     *
     * When the value of this property is true, the SDK will direct write operations to
     * available writable locations of geo-replicated database account. Writable locations
     * are ordered by PreferredLocations property. Setting the property value
     * to true has no effect until EnableMultipleWriteLocations in DatabaseAccount
     * is also set to true.
     *
     * Default value is false indicating that writes are directed to
     * first region in PreferredLocations property if it's a write region or the primary account region if no PreferredLocations are specified. 
     *
     * The value should match your account configuration.
     * 
     * During the client lifetime, writes can change regional endpoint in the case of any event described at https://docs.microsoft.com/azure/cosmos-db/troubleshoot-sdk-availability
     *
     * @return flag to enable writes on any locations (regions) for geo-replicated database accounts.
     */
    public boolean isUsingMultipleWriteLocations() {
        return this.usingMultipleWriteLocations;
    }

    /**
     * Gets whether to allow for reads to go to multiple regions configured on an account of Azure Cosmos DB service.
     *
     * Default value is null.
     *
     * If this property is not set, the default is true for all Consistency Levels other than Bounded Staleness,
     * The default is false for Bounded Staleness.
     * 1. {@link #enableEndpointDiscovery} is true
     * 2. the Azure Cosmos DB account has more than one region
     *
     * @return flag to allow for reads to go to multiple regions configured on an account of Azure Cosmos DB service.
     */
    public Boolean isEnableReadRequestsFallback() {
        return this.enableReadRequestsFallback;
    }

    /**
     * Sets the flag to enable writes on any locations (regions) for geo-replicated database accounts in the Azure Cosmos DB service.
     *
     * When the value of this property is true, the SDK will direct write operations to
     * available writable locations of geo-replicated database account. Writable locations
     * are ordered by PreferredLocations property. Setting the property value
     * to true has no effect until EnableMultipleWriteLocations in DatabaseAccount
     * is also set to true.
     *
     * Default value is false indicating that writes are directed to
     * first region in PreferredLocations property if it's a write region or the primary account region if no PreferredLocations are specified. 
     *
     * The value should match your account configuration.
     * 
     * During the client lifetime, writes can change regional endpoint in the case of any event described at https://docs.microsoft.com/azure/cosmos-db/troubleshoot-sdk-availability
     *
     * @param usingMultipleWriteLocations flag to enable writes on any locations (regions) for geo-replicated database accounts.
     */
    public void setUsingMultipleWriteLocations(boolean usingMultipleWriteLocations) {
        this.usingMultipleWriteLocations = usingMultipleWriteLocations;
    }

    /**
     * Sets whether to allow for reads to go to multiple regions configured on an account of Azure Cosmos DB service.
     *
     * Default value is null.
     *
     * If this property is not set, the default is true for all Consistency Levels other than Bounded Staleness,
     * The default is false for Bounded Staleness.
     * 1. {@link #enableEndpointDiscovery} is true
     * 2. the Azure Cosmos DB account has more than one region
     *
     * @param enableReadRequestsFallback flag to enable reads to go to multiple regions configured on an account of Azure Cosmos DB service.
     */
    public void setEnableReadRequestsFallback(Boolean enableReadRequestsFallback) {
        this.enableReadRequestsFallback = enableReadRequestsFallback;
    }

    /**
     * Gets the preferred locations for geo-replicated database accounts
     *
     * @return the list of preferred location.
     */
    public List<String> getPreferredLocations() {
        return this.preferredLocations != null ? preferredLocations : Collections.emptyList();
    }

    /**
     * Sets the preferred locations for geo-replicated database accounts. For example,
     * "East US" as the preferred location.
     * <p>
     * When EnableEndpointDiscovery is true and PreferredRegions is non-empty,
     * the SDK will prefer to use the locations in the collection in the order
     * they are specified to perform operations.
     * <p>
     * If EnableEndpointDiscovery is set to false, this property is ignored.
     *
     * @param preferredLocations the list of preferred locations.
     */
    public void setPreferredLocations(List<String> preferredLocations) {
        this.preferredLocations = preferredLocations;
    }

    /**
     * Gets the InetSocketAddress of proxy server.
     *
     * @return the value of proxyHost.
     */
    public InetSocketAddress getProxy() {
        return this.inetSocketProxyAddress;
    }

    /**
     * This will create the InetSocketAddress for proxy server,
     * all the requests to cosmoDB will route from this address.
     * @param proxyHost The proxy server host.
     * @param proxyPort The proxy server port.
     */
    public void setProxy(String proxyHost, int proxyPort) {
        this.inetSocketProxyAddress = new InetSocketAddress(proxyHost, proxyPort);
    }

    @Override
    public String toString() {
        return "ConnectionPolicy{" +
                "requestTimeoutInMillis=" + requestTimeoutInMillis +
                ", mediaRequestTimeoutInMillis=" + mediaRequestTimeoutInMillis +
                ", connectionMode=" + connectionMode +
                ", mediaReadMode=" + mediaReadMode +
                ", maxPoolSize=" + maxPoolSize +
                ", idleConnectionTimeoutInMillis=" + idleConnectionTimeoutInMillis +
                ", userAgentSuffix='" + userAgentSuffix + '\'' +
                ", retryOptions=" + retryOptions +
                ", enableEndpointDiscovery=" + enableEndpointDiscovery +
                ", preferredLocations=" + preferredLocations +
                ", usingMultipleWriteLocations=" + usingMultipleWriteLocations +
                ", inetSocketProxyAddress=" + inetSocketProxyAddress +
                '}';
    }
}
