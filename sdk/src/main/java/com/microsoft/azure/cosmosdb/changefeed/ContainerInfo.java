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
package com.microsoft.azure.cosmosdb.changefeed;

import com.microsoft.azure.cosmosdb.ConnectionPolicy;
import com.microsoft.azure.cosmosdb.ConsistencyLevel;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Holds information specifying how to get the Cosmos container.
 */
public class ContainerInfo {
    private ConnectionPolicy connectionPolicy;
    private ConsistencyLevel consistencyLevel;
    private String uri;
    private String masterKey;
    private String databaseName;
    private String containerName;

    /**
     * Initializes a new instance of the {@link ContainerInfo} class.
     */
    public ContainerInfo() {
        this.connectionPolicy = new ConnectionPolicy();
        this.consistencyLevel = ConsistencyLevel.Session;
    }

    /**
     * Initializes a new instance of the {@link ContainerInfo} class.
     *
     * @param containerInfo the {@link ContainerInfo} instance to copy the settings from.
     */
    public ContainerInfo(ContainerInfo containerInfo) {
        this.connectionPolicy = containerInfo.connectionPolicy;
        this.consistencyLevel = containerInfo.consistencyLevel;
        this.uri = containerInfo.uri;
        this.masterKey = containerInfo.masterKey;
        this.databaseName = containerInfo.databaseName;
        this.containerName = containerInfo.containerName;
    }

    /**
     * Gets the connection policy to connect to Cosmos service.
     *
     * @return the connection policy to connect to Cosmos service.
     */
    public ConnectionPolicy getConnectionPolicy() {
        return this.connectionPolicy;
    }

    /**
     * Gets the consistency level; default is "Session".
     * @return the consistency level.
     */
    public ConsistencyLevel getConsistencyLevel() {
        return this.consistencyLevel;
    }

    /**
     * Gets the URI of the Document service.
     *
     * @return the URI of the Document service.
     */
    public String getUri() {
        return this.uri;
    }

    /**
     * Gets the secret master key to connect to the Cosmos service.
     *
     * @return the secret master key to connect to the Cosmos service.
     */
    public String getMasterKey() {
        return this.masterKey;
    }

    /**
     * Gets the name of the database the container resides in.
     *
     * @return the name of the database the container resides in.
     */
    public String getDatabaseName() {
        return this.databaseName;
    }

    /**
     * Gets the name of the Cosmos container.
     *
     * @return the name of the Cosmos container.
     */
    public String getContainerName() {
        return this.containerName;
    }

    /**
     * Sets the connection policy to connect to Cosmos service.
     *
     * @param connectionPolicy the connection policy to connect to Cosmos service.
     * @return current instance of {@link ContainerInfo}
     */
    public ContainerInfo withConnectionPolicy(ConnectionPolicy connectionPolicy) {
        this.connectionPolicy = connectionPolicy;
        return this;
    }

    /**
     * Sets the consistency level.
     *
     * @param consistencyLevel the consistency level.
     * @return current instance of {@link ContainerInfo}
     */
    public ContainerInfo withConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
        return this;
    }

    /**
     * Sets the URI of the Document service.
     * @param uri the URI of the Cosmos service.
     * @return current instance of {@link ContainerInfo}
     */
    public ContainerInfo withUri(String uri) {
        try {
            new URI(uri);
        } catch (URISyntaxException ex) {
            throw new IllegalArgumentException("uri");
        }

        this.uri = uri;
        return this;
    }

    /**
     * Sets the secret master key to connect to the Cosmos service.
     * @param masterKey the secret master key to connect to the Cosmos service.
     * @return current instance of {@link ContainerInfo}
     */
    public ContainerInfo withMasterKey(String masterKey) {
        this.masterKey = masterKey;
        return this;
    }

    /**
     * Sets the name of the database the container resides in.
     *
     * @param databaseName the name of the database the container resides in.
     * @return current instance of {@link ContainerInfo}
     */
    public ContainerInfo withDatabaseName(String databaseName) {
        this.databaseName = databaseName;
        return this;
    }

    /**
     * Sets the name of the Cosmos container.
     *
     * @param containerName the name of the Cosmos container.
     * @return current instance of {@link ContainerInfo}
     */
    public ContainerInfo withContainerName(String containerName) {
        this.containerName = containerName;
        return this;
    }
}
