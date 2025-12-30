/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.external.util.iceberg;

import java.util.List;

import org.apache.asterix.external.util.aws.AwsConstants;
import org.apache.asterix.external.util.azure.AzureConstants;
import org.apache.asterix.external.util.google.GCSConstants;

public class IcebergConstants {
    private IcebergConstants() {
        throw new AssertionError("do not instantiate");
    }

    public static final String ICEBERG_URI_PROPERTY_KEY = "uri";
    public static final String ICEBERG_TABLE_FORMAT = "iceberg";
    public static final String ICEBERG_CATALOG_NAME = "catalogName";
    public static final String ICEBERG_SOURCE_PROPERTY_KEY = "catalogSource";
    public static final String ICEBERG_WAREHOUSE_PROPERTY_KEY = "warehouse";
    public static final String ICEBERG_TABLE_NAME_PROPERTY_KEY = "tableName";
    public static final String ICEBERG_NAMESPACE_PROPERTY_KEY = "namespace";
    public static final String ICEBERG_SNAPSHOT_ID_PROPERTY_KEY = "snapshotId";
    public static final String ICEBERG_SCHEMA_ID_PROPERTY_KEY = "schemaId";
    public static final String ICEBERG_SNAPSHOT_TIMESTAMP_PROPERTY_KEY = "snapshotTimestamp";

    public static final String ICEBERG_CATALOG_PROPERTY_PREFIX_INTERNAL = "catalog-property#";
    public static final String ICEBERG_COLLECTION_PROPERTY_PREFIX_INTERNAL = "collection-property#";
    public static final String ICEBERG_IO_READER_TYPE = "icebergIoReaderType";

    public static final String ICEBERG_PARQUET_FORMAT = "iceberg-parquet";
    public static final String ICEBERG_AVRO_FORMAT = "iceberg-avro";

    public static class Aws {
        public static final String REST_SIG4_SIGNING_REGION_PROPERTY_NAME = "sigv4SigningRegion";
        public static final String REST_SIG4_SIGNING_NAME_PROPERTY_NAME = "sigv4SigningName";
        public static final String REST_SIG4_GLUE_SIGNING_NAME = "glue";

        // catalog properties
        public static final String S3_FILE_IO = "org.apache.iceberg.aws.s3.S3FileIO";
        public static final String REST_SIG4_SIGNING_NAME = "rest.signing-name";
        public static final String REST_SIG4_SIGNING_REGION = "rest.signing-region";
    }

    public static class Gcp {
        public static final String QUOTA_PROJECT_ID_HEADER_KEY = "header.x-goog-user-project";
        public static final String QUOTA_PROJECT_ID_KEY = "quotaProjectId";
    }

    public static class Rest {

    }

    // we need to split the creds for the catalog from the iceberg table, we will prefix the
    // catalog auths with ICEBERG_CATALOG_PROPERTY_PREFIX_INTERNAL
    public static final List<String> authParams =
            java.util.stream.Stream.of(AwsConstants.authParams, GCSConstants.authParams, AzureConstants.authParams)
                    .flatMap(java.util.Collection::stream).toList();
}
