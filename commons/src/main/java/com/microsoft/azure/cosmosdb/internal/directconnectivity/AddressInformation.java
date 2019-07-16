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

package com.microsoft.azure.cosmosdb.internal.directconnectivity;

import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

/**
 * Used internally to encapsulate a physical address information in the Azure Cosmos DB database service.
 */
public class AddressInformation {
    private Protocol protocol;
    private boolean isPublic;
    private boolean isPrimary;
    private Uri physicalUri;

    public AddressInformation(boolean isPublic, boolean isPrimary, String physicalUri, Protocol protocol) {
        Objects.requireNonNull(protocol);
        this.protocol = protocol;
        this.isPublic = isPublic;
        this.isPrimary = isPrimary;
        this.physicalUri = new Uri(normalizePhysicalUri(physicalUri));
    }

    private static String normalizePhysicalUri(String physicalUri) {
        if (StringUtils.isEmpty(physicalUri)) {
            return physicalUri;
        }

        // backend returns non normalized uri with "//" tail
        // e.g, https://cdb-ms-prod-westus2-fd2.documents.azure.com:15248/apps/4f5c042d-76fb-4ce6-bda3-517e6ef3984f/
        // services/cf4b9ab2-019c-45ca-ac88-25a92b66dddf/partitions/2078862a-d698-475b-a308-02598370d1d9/replicas/132077748219659199s//
        // we should trim the tail double "//"

        int i = physicalUri.length() -1;

        while(i >= 0 && physicalUri.charAt(i) == '/') {
            i--;
        }

        return physicalUri.substring(0, i) + '/';
    }

    public AddressInformation(boolean isPublic, boolean isPrimary, String physicalUri, String protocolScheme) {
        this(isPublic, isPrimary, physicalUri, scheme2protocol(protocolScheme));
    }

    public boolean isPublic() {
        return isPublic;
    }

    public boolean isPrimary() {
        return isPrimary;
    }

    public Uri getPhysicalUri() {
        return physicalUri;
    }

    public Protocol getProtocol() {
        return this.protocol;
    }

    public String getProtocolName() {
        return this.protocol.name();
    }

    public String getProtocolScheme() {
        return this.protocol.scheme();
    }

    @Override
    public String toString() {
        return "AddressInformation{" +
                "protocol='" + protocol + '\'' +
                ", isPublic=" + isPublic +
                ", isPrimary=" + isPrimary +
                ", physicalUri='" + physicalUri + '\'' +
                '}';
    }

    private static Protocol scheme2protocol(String scheme) {

        Objects.requireNonNull(scheme, "scheme");

        switch (scheme.toLowerCase()) {
            case "https":
                return Protocol.Https;
            case "rntbd":
                return Protocol.Tcp;
            default:
                throw new IllegalArgumentException(String.format("scheme: %s", scheme));
        }
    }
}
