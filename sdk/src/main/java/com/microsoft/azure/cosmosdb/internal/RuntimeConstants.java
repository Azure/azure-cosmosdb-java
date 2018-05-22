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

package com.microsoft.azure.cosmosdb.internal;

/**
 * Used internally. Runtime constants in the Azure Cosmos DB database service Java SDK.
 */
public class RuntimeConstants {
    public static class MediaTypes {
        // http://www.iana.org/assignments/media-types/media-types.xhtml
        public static final String ANY = "*/*";
        public static final String IMAGE_JPEG = "image/jpeg";
        public static final String IMAGE_PNG = "image/png";
        public static final String JAVA_SCRIPT = "application/x-javascript";
        public static final String JSON = "application/json";
        public static final String OCTET_STREAM = "application/octet-stream";
        public static final String QUERY_JSON = "application/query+json";
        public static final String SQL = "application/sql";
        public static final String TEXT_HTML = "text/html";
        public static final String TEXT_PLAIN = "text/plain";
        public static final String XML = "application/xml";
    }

    public static class Protocols {
        public static final String HTTPS = "https";
    }

    static class Separators {
        static final char[] URL = new char[] {'/'};
        static final char[] QUOTE = new char[] {'\''};
        static final char[] DOMAIN_ID = new char[] {'-'};
        static final char[] QUERY = new char[] {'?', '&', '='};
        static final char[] PARENTHESIS = new char[] {'(', ')'};
        static final char[] USER_AGENT_HEADER = new char[] {'(', ')', ';', ','};


        //Note that the accept header separator here is ideally comma. Semicolon is used for separators within individual
        //header for now cloud moe does not recognize such accept header hence we allow both semicolon or comma separated
        //accept header
        static final char[] HEADER = new char[] {';', ','};
        static final char[] COOKIE_SEPARATOR = new char[] {';'};
        static final char[] COOKIE_VALUE_SEPARATOR = new char[] {'='};
        static final char[] PPM_USER_TOKEN = new char[] {':'};
        static final char[] IDENTIFIER = new char[] {'-'};
        static final char[] HOST = new char[] {'.'};
        static final char[] VERSION = new char[] {','};
        static final char[] PAIR = new char[] {';'};
        static final char[] E_TAG = new char[] {'#'};
        static final char[] MEMBER_QUERY = new char[] {'+'};

        static final String HEADER_ENCODING_BEGIN = "=?";
        static final String HEADER_ENCODING_END = "?=";
        static final String HEADER_ENCODING_SEPARATOR = "?";
    }
}
