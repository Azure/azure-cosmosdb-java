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
package com.microsoft.azure.cosmosdb.rx.internal.query;

import com.microsoft.azure.cosmosdb.JsonSerializable;
import com.microsoft.azure.cosmosdb.rx.internal.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistinctContinuationToken extends JsonSerializable {

    private static final String LAST_HASH_PROPERTY_NAME = "lastHash";
    private static final String SOURCE_TOKEN_PROPERTY_NAME = "sourceToken";

    private static final Logger logger = LoggerFactory.getLogger(TakeContinuationToken.class);

    public DistinctContinuationToken(String lastHash, String sourceToken) {
        this.setLastHash(lastHash);
        this.setSourceToken(sourceToken);
    }

    private DistinctContinuationToken(String serializedDistinctContinuationToken) {
        super(serializedDistinctContinuationToken);
    }

    public static boolean tryParse(String serializedDistinctContinuationToken,
                                   Utils.ValueHolder<DistinctContinuationToken> outDistinctContinuationToken) {

        boolean parsed;
        try {
            DistinctContinuationToken distinctContinuationToken =
                    new DistinctContinuationToken(serializedDistinctContinuationToken);
            distinctContinuationToken.getSourceToken();
            distinctContinuationToken.getLastHash();
            outDistinctContinuationToken.v = distinctContinuationToken;
            parsed = true;
        } catch (Exception ex) {
            logger.debug(
                    "Received exception {} when trying to parse: {}",
                    ex.getMessage(),
                    serializedDistinctContinuationToken);
            parsed = false;
            outDistinctContinuationToken.v = null;
        }

        return parsed;
    }

    String getSourceToken() {
        return super.getString(SOURCE_TOKEN_PROPERTY_NAME);
    }

    /**
     * Setter for property 'sourceToken'.
     *
     * @param sourceToken Value to set for property 'sourceToken'.
     */
    public void setSourceToken(String sourceToken) {
        super.set(SOURCE_TOKEN_PROPERTY_NAME, sourceToken);
    }

    String getLastHash() {
        return super.getString(LAST_HASH_PROPERTY_NAME);
    }

    /**
     * Setter for property 'lastHash'.
     *
     * @param lastHash Value to set for property 'lastHash'.
     */
    public void setLastHash(String lastHash) {
        super.set(LAST_HASH_PROPERTY_NAME, lastHash);
    }

}
