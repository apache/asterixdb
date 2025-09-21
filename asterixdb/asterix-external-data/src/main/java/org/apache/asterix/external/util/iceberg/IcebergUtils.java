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

import static org.apache.asterix.common.exceptions.ErrorCode.PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT;
import static org.apache.asterix.common.exceptions.ErrorCode.UNSUPPORTED_ICEBERG_DATA_FORMAT;
import static org.apache.asterix.external.util.iceberg.IcebergConstants.ICEBERG_AVRO_FORMAT;
import static org.apache.asterix.external.util.iceberg.IcebergConstants.ICEBERG_PARQUET_FORMAT;
import static org.apache.asterix.external.util.iceberg.IcebergConstants.ICEBERG_SNAPSHOT_ID_PROPERTY_KEY;
import static org.apache.asterix.external.util.iceberg.IcebergConstants.ICEBERG_SNAPSHOT_TIMESTAMP_PROPERTY_KEY;
import static org.apache.asterix.external.util.iceberg.IcebergConstants.ICEBERG_TABLE_FORMAT;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.asterix.common.config.CatalogConfig;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.external.util.aws.glue.GlueUtils;
import org.apache.asterix.external.util.google.biglake_metastore.BiglakeMetastore;
import org.apache.asterix.om.types.ARecordType;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Catalog;

public class IcebergUtils {

    /**
     * Checks if the provided catalog is an Iceberg catalog
     *
     * @param catalogType catalog type
     * @return true if Iceberg catalog, false otherwise
     */
    public static boolean isIcebergCatalog(String catalogType) {
        if (catalogType == null) {
            return false;
        }
        return catalogType.equalsIgnoreCase(CatalogConfig.CatalogType.ICEBERG.name());
    }

    /**
     * Checks if the provided configuration is for Iceberg table
     *
     * @param configuration external data configuration
     */
    public static boolean isIcebergTable(Map<String, String> configuration) {
        String tableFormat = configuration.get(ExternalDataConstants.TABLE_FORMAT);
        if (tableFormat != null) {
            return tableFormat.equals(ICEBERG_TABLE_FORMAT);
        }
        return false;
    }

    public static void validateCatalogProperties(Map<String, String> properties) throws CompilationException {
        String catalogSource = properties.get(IcebergConstants.ICEBERG_SOURCE_PROPERTY_KEY);
        if (catalogSource == null) {
            throw new CompilationException(ErrorCode.ICEBERG_CATALOG_SOURCE_REQUIRED);
        }
        validateCatalogSource(catalogSource);
        validatePropertyPresent(properties, IcebergConstants.ICEBERG_SOURCE_PROPERTY_KEY);
        validatePropertyPresent(properties, IcebergConstants.ICEBERG_WAREHOUSE_PROPERTY_KEY);
    }

    /**
     * Checks if the provided iceberg catalog source is a supported iceberg catalog type
     *
     * @param catalogSource catalog source
     * @throws CompilationException CompilationException
     */
    public static void validateCatalogSource(String catalogSource) throws CompilationException {
        for (CatalogConfig.IcebergCatalogSource icebergSource : CatalogConfig.IcebergCatalogSource.values()) {
            if (icebergSource.name().equalsIgnoreCase(catalogSource)) {
                return;
            }
        }
        throw new CompilationException(ErrorCode.UNSUPPORTED_ICEBERG_CATALOG_SOURCE, catalogSource);
    }

    private static void validatePropertyPresent(Map<String, String> properties, String key)
            throws CompilationException {
        String property = properties.get(key);
        if (property == null || property.isEmpty()) {
            throw new CompilationException(ErrorCode.PARAMETERS_REQUIRED, key);
        }
    }

    public static void validateIcebergTableProperties(Map<String, String> properties) throws CompilationException {
        // required table name
        String tableName = properties.get(IcebergConstants.ICEBERG_TABLE_NAME_PROPERTY_KEY);
        if (tableName == null || tableName.isEmpty()) {
            throw new CompilationException(ErrorCode.PARAMETERS_REQUIRED,
                    IcebergConstants.ICEBERG_TABLE_NAME_PROPERTY_KEY);
        }

        // required namespace
        String namespace = properties.get(IcebergConstants.ICEBERG_NAMESPACE_PROPERTY_KEY);
        if (namespace == null || namespace.isEmpty()) {
            throw new CompilationException(ErrorCode.PARAMETERS_REQUIRED,
                    IcebergConstants.ICEBERG_NAMESPACE_PROPERTY_KEY);
        }

        // validate snapshot id and timestamp
        String snapshotId = properties.get(ICEBERG_SNAPSHOT_ID_PROPERTY_KEY);
        String snapshotTimestamp = properties.get(ICEBERG_SNAPSHOT_TIMESTAMP_PROPERTY_KEY);
        if (snapshotId != null && snapshotTimestamp != null) {
            throw new CompilationException(PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT,
                    ICEBERG_SNAPSHOT_TIMESTAMP_PROPERTY_KEY, ICEBERG_SNAPSHOT_ID_PROPERTY_KEY);
        }

        try {
            if (snapshotId != null) {
                Long.parseLong(snapshotId);
            } else if (snapshotTimestamp != null) {
                Long.parseLong(snapshotTimestamp);
            }
        } catch (NumberFormatException e) {
            throw new CompilationException(ErrorCode.INVALID_ICEBERG_SNAPSHOT_VALUE,
                    snapshotId != null ? snapshotId : snapshotTimestamp);
        }
    }

    /**
     * Extracts and returns the iceberg catalog properties from the provided configuration
     *
     * @param configuration configuration
     * @return catalog properties
     */
    public static Map<String, String> filterCatalogProperties(Map<String, String> configuration) {
        Map<String, String> catalogProperties = new HashMap<>();
        Iterator<Map.Entry<String, String>> iterator = configuration.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            if (entry.getKey().startsWith(IcebergConstants.ICEBERG_PROPERTY_PREFIX_INTERNAL)) {
                catalogProperties.put(
                        entry.getKey().substring(IcebergConstants.ICEBERG_PROPERTY_PREFIX_INTERNAL.length()),
                        entry.getValue());
                iterator.remove();
            }
        }
        return catalogProperties;
    }

    /**
     * Namespace can be null (not passed), or it can be passed for the catalog or the collection. If it is passed
     * for both, namespace for the collection will be used, otherwise, the namespace for the catalog will be used.
     *
     * @param configuration configuration
     * @return namespace
     */
    public static String getNamespace(Map<String, String> configuration) {
        String namespace = configuration.get(IcebergConstants.ICEBERG_NAMESPACE_PROPERTY_KEY);
        if (namespace != null) {
            return namespace;
        }

        String catalogNamespaceProperty =
                IcebergConstants.ICEBERG_PROPERTY_PREFIX_INTERNAL + IcebergConstants.ICEBERG_NAMESPACE_PROPERTY_KEY;
        namespace = configuration.get(catalogNamespaceProperty);
        return namespace;
    }

    public static String getIcebergFormat(Map<String, String> configuration) throws AsterixException {
        String format = configuration.get(ExternalDataConstants.KEY_FORMAT).toLowerCase();
        return switch (format) {
            case ExternalDataConstants.FORMAT_PARQUET -> ICEBERG_PARQUET_FORMAT;
            case ExternalDataConstants.FORMAT_AVRO -> ICEBERG_AVRO_FORMAT;
            default -> throw AsterixException.create(UNSUPPORTED_ICEBERG_DATA_FORMAT, format);
        };
    }

    public static Catalog initializeCatalog(Map<String, String> catalogProperties, String namespace)
            throws CompilationException {
        String source = catalogProperties.get(IcebergConstants.ICEBERG_SOURCE_PROPERTY_KEY);
        Optional<CatalogConfig.IcebergCatalogSource> catalogSource = CatalogConfig.getIcebergCatalogSource(source);
        if (catalogSource.isEmpty()) {
            throw CompilationException.create(ErrorCode.UNSUPPORTED_ICEBERG_CATALOG_SOURCE, source);
        }

        // remove null values to avoid failures in internal checks
        catalogProperties.values().removeIf(Objects::isNull);
        return switch (catalogSource.get()) {
            case CatalogConfig.IcebergCatalogSource.AWS_GLUE -> GlueUtils.initializeCatalog(catalogProperties, namespace);
            case CatalogConfig.IcebergCatalogSource.BIGLAKE_METASTORE -> BiglakeMetastore.initializeCatalog(catalogProperties, namespace);
            case CatalogConfig.IcebergCatalogSource.REST -> null;
        };
    }

    public static void closeCatalog(Catalog catalog) throws CompilationException {
        if (catalog != null) {
            if (catalog instanceof GlueCatalog) {
                GlueUtils.closeCatalog((GlueCatalog) catalog);
            }
        }
    }

    public static boolean snapshotIdExists(Table table, long snapshot) {
        return table.snapshot(snapshot) != null;
    }

    public static String[] getProjectedFields(Map<String, String> configuration) throws IOException {
        String encoded = configuration.get(ExternalDataConstants.KEY_REQUESTED_FIELDS);
        ARecordType projectedRecordType = ExternalDataUtils.getExpectedType(encoded);
        return projectedRecordType.getFieldNames();
    }

    /**
     * Sets the default format to Parquet if the format is not provided for Iceberg tables
     * @param configuration configuration
     */
    public static void setDefaultFormat(Map<String, String> configuration) {
        if (IcebergUtils.isIcebergTable(configuration) && configuration.get(ExternalDataConstants.KEY_FORMAT) == null) {
            configuration.put(ExternalDataConstants.KEY_FORMAT, ExternalDataConstants.FORMAT_PARQUET);
        }
    }
}
