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

class WFConstants {
    static final int DEFAULT_FABRIC_NAME_RESOLUTION_TIMEOUT_IN_SECONDS = 10;

    static class AzureNames {
        public static final String WORKER_ROLE_NAME = "WAFab";
        public static final String NODE_ADDRESS_ENDPOINT = "NodeAddress";
    }

    static class WireNames {
        public static final String NAMED_ENDPOINT = "App=";
        public static final String NAMESPACE = "http://docs.core.windows.net";
        public static final String REQUEST = "Request";
        public static final String RESPONSE = "Response";
        public static final String REQUEST_ACTION = "http://docs.core.windows.net/DocDb/Invoke";
        public static final String RESPONSE_ACTION = "http://docs.core.windows.net/DocDb/InvokeResponse";

        public static final String ADDRESSING_HEADER_NAMESPACE = "http://www.w3.org/2005/08/addressing";
        public static final String MESSAGE_ID_HEADER_NAME = "MessageID";
        public static final String RELATES_TO_HEADER_NAME = "RelatesTo";
    }

    static class BackendHeaders {
        public static final String RESOURCE_ID = "x-docdb-resource-id";
        public static final String OWNER_ID = "x-docdb-owner-id";
        public static final String ENTITY_ID = "x-docdb-entity-id";
        public static final String DATABASE_ENTITY_MAX_COUNT = "x-ms-database-entity-max-count";
        public static final String DATABASE_ENTITY_CURRENT_COUNT = "x-ms-database-entity-current-count";
        public static final String COLLECTION_ENTITY_MAX_COUNT = "x-ms-collection-entity-max-count";
        public static final String COLLECTION_ENTITY_CURRENT_COUNT = "x-ms-collection-entity-current-count";
        public static final String USER_ENTITY_MAX_COUNT = "x-ms-user-entity-max-count";
        public static final String USER_ENTITY_CURRENT_COUNT = "x-ms-user-entity-current-count";
        public static final String PERMISSION_ENTITY_MAX_COUNT = "x-ms-permission-entity-max-count";
        public static final String PERMISSION_ENTITY_CURRENT_COUNT = "x-ms-permission-entity-current-count";
        public static final String ROOT_ENTITY_MAX_COUNT = "x-ms-root-entity-max-count";
        public static final String ROOT_ENTITY_CURRENT_COUNT = "x-ms-root-entity-current-count";
        public static final String RESOURCE_SCHEMA_NAME = "x-ms-resource-schema-name";
        public static final String LSN = "lsn";
        public static final String QUORUM_ACKED_LSN = "x-ms-quorum-acked-lsn";
        public static final String CURRENT_WRITE_QUORUM = "x-ms-current-write-quorum";
        public static final String CURRENT_REPLICA_SET_SIZE = "x-ms-current-replica-set-size";
        public static final String COLLECTION_PARTITION_INDEX = "collection-partition-index";
        public static final String COLLECTION_SERVICE_INDEX = "collection-service-index";
        public static final String STATUS = "Status";
        public static final String ACTIVITY_ID = "ActivityId";
        public static final String IS_FANOUT_REQUEST = "x-ms-is-fanout-request";
        public static final String PRIMARY_MASTER_KEY = "x-ms-primary-master-key";
        public static final String SECONDARY_MASTER_KEY = "x-ms-secondary-master-key";
        public static final String PRIMARY_READONLY_KEY = "x-ms-primary-readonly-key";
        public static final String SECONDARY_READONLY_KEY = "x-ms-secondary-readonly-key";
        public static final String BIND_REPLICA_DIRECTIVE = "x-ms-bind-replica";
        public static final String DATABASE_ACCOUNT_ID = "x-ms-database-account-id";
        public static final String REQUEST_VALIDATION_FAILURE = "x-ms-request-validation-failure";
        public static final String SUB_STATUS = "x-ms-substatus";
        public static final String PARTITION_KEY_RANGE_ID = "x-ms-documentdb-partitionkeyrangeid";
        public static final String BIND_MIN_EFFECTIVE_PARTITION_KEY = "x-ms-documentdb-bindmineffectivepartitionkey";
        public static final String BIND_MAX_EFFECTIVE_PARTITION_KEY = "x-ms-documentdb-bindmaxeffectivepartitionkey";
        public static final String BIND_PARTITION_KEY_RANGE_ID = "x-ms-documentdb-bindpartitionkeyrangeid";
        public static final String BIND_PARTITION_KEY_RANGE_RID_PREFIX = "x-ms-documentdb-bindpartitionkeyrangeridprefix";
        public static final String MINIMUM_ALLOWED_CLIENT_VERSION = "x-ms-documentdb-minimumallowedclientversion";
        public static final String PARTITION_COUNT = "x-ms-documentdb-partitioncount";
        public static final String COLLECTION_RID = "x-ms-documentdb-collection-rid";
        public static final String GLOBAL_COMMITTED_LSN = "x-ms-global-committed-lsn";
        public static final String NUMBER_OF_READ_REGIONS = "x-ms-number-of-read-regions";
        public static final String ITEM_LSN = "x-ms-item-lsn";
    }
}
