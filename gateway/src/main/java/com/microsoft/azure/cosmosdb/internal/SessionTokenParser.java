package com.microsoft.azure.cosmosdb.internal;

import com.microsoft.azure.cosmosdb.rx.internal.BadRequestException;
import com.microsoft.azure.cosmosdb.rx.internal.RMResources;
import com.microsoft.azure.cosmosdb.rx.internal.Strings;
import com.microsoft.azure.cosmosdb.rx.internal.Utils;
import org.apache.commons.lang3.StringUtils;

public class SessionTokenParser {

    public static ISessionToken parse(String sessionToken) {
        Utils.ValueHolder<ISessionToken> partitionKeyRangeSessionToken = Utils.ValueHolder.initialize(null);

        if (SessionTokenParser.tryParse(sessionToken, partitionKeyRangeSessionToken)) {
            return partitionKeyRangeSessionToken.v;
        } else {
            throw new  RuntimeException(new BadRequestException(String.format(RMResources.InvalidSessionToken, sessionToken)));
        }
    }

    static boolean tryParse(String sessionToken, Utils.ValueHolder<ISessionToken> parsedSessionToken) {
        parsedSessionToken.v = null;
        if (!Strings.isNullOrEmpty(sessionToken)) {
            String[] sessionTokenSegments = StringUtils.split(sessionToken,":");
            return VectorSessionToken.tryCreate(sessionTokenSegments[sessionTokenSegments.length - 1], parsedSessionToken);
        } else {
            return false;
        }
    }


}
