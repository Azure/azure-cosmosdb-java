package com.microsoft.azure.cosmosdb.rx.internal.query;

import com.microsoft.azure.cosmosdb.JsonSerializable;
import com.microsoft.azure.cosmosdb.rx.internal.Utils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OffsetContinuationToken extends JsonSerializable {
    private static final String TOKEN_PROPERTY_NAME = "sourceToken";
    private static final String OFFSET_PROPERTY_NAME = "offset";
    private static final Logger logger = LoggerFactory.getLogger(CompositeContinuationToken.class);

    public OffsetContinuationToken(int offset, String sourceToken) {

        if (offset < 0) {
            throw new IllegalArgumentException("offset should be non negative");
        }

        this.setOffset(offset);
        this.setSourceToken(sourceToken);
    }

    public OffsetContinuationToken(String serializedCompositeToken) {
        super(serializedCompositeToken);
        this.getOffset();
        this.getSourceToken();
    }

    private void setSourceToken(String sourceToken) {
        super.set(TOKEN_PROPERTY_NAME, sourceToken);
    }

    private void setOffset(int offset) {
        super.set(OFFSET_PROPERTY_NAME, offset);
    }

    public String getSourceToken() {
        return super.getString(TOKEN_PROPERTY_NAME);
    }

    public int getOffset() {
        if (super.getInt(OFFSET_PROPERTY_NAME) == null) {
            return 0;
        }
        return super.getInt(OFFSET_PROPERTY_NAME);
    }

    public static boolean tryParse(String serializedOffsetContinuationToken,
                                   Utils.ValueHolder<OffsetContinuationToken> outOffsetContinuationToken) {
        if (StringUtils.isEmpty(serializedOffsetContinuationToken)) {
            return false;
        }

        boolean parsed;
        try {
            outOffsetContinuationToken.v = new OffsetContinuationToken(serializedOffsetContinuationToken);
            parsed = true;
        } catch (Exception ex) {
            logger.debug("Received exception {} when trying to parse: {}",
                    ex.getMessage(),
                    serializedOffsetContinuationToken);
            parsed = false;
            outOffsetContinuationToken.v = null;
        }

        return parsed;
    }

}
