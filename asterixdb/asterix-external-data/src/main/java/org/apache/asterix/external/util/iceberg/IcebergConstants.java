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

public class IcebergConstants {
    private IcebergConstants() {
        throw new AssertionError("do not instantiate");
    }

    public static final String ICEBERG_TABLE_FORMAT = "iceberg";
    public static final String ICEBERG_CATALOG_NAME = "catalogName";
    public static final String ICEBERG_SOURCE_PROPERTY_KEY = "catalogSource";
    public static final String ICEBERG_WAREHOUSE_PROPERTY_KEY = "warehouse";
    public static final String ICEBERG_TABLE_NAME_PROPERTY_KEY = "tableName";
    public static final String ICEBERG_NAMESPACE_PROPERTY_KEY = "namespace";
    public static final String ICEBERG_SNAPSHOT_ID_PROPERTY_KEY = "snapshotId";
    public static final String ICEBERG_SCHEMA_ID_PROPERTY_KEY = "schemaId";
    public static final String ICEBERG_SNAPSHOT_TIMESTAMP_PROPERTY_KEY = "snapshotTimestamp";

    public static final String ICEBERG_PROPERTY_PREFIX_INTERNAL = "catalog-property#";

    public static final String ICEBERG_PARQUET_FORMAT = "iceberg-parquet";
    public static final String ICEBERG_AVRO_FORMAT = "iceberg-avro";

    public class Aws {
        public static final String S3_FILE_IO = "org.apache.iceberg.aws.s3.S3FileIO";
    }
}
