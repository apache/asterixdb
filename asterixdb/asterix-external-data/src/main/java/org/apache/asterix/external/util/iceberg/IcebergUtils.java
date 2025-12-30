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
import static org.apache.asterix.external.util.aws.EnsureCloseClientsFactoryRegistry.FACTORY_INSTANCE_ID_KEY;
import static org.apache.asterix.external.util.iceberg.IcebergConstants.ICEBERG_AVRO_FORMAT;
import static org.apache.asterix.external.util.iceberg.IcebergConstants.ICEBERG_CATALOG_PROPERTY_PREFIX_INTERNAL;
import static org.apache.asterix.external.util.iceberg.IcebergConstants.ICEBERG_COLLECTION_PROPERTY_PREFIX_INTERNAL;
import static org.apache.asterix.external.util.iceberg.IcebergConstants.ICEBERG_PARQUET_FORMAT;
import static org.apache.asterix.external.util.iceberg.IcebergConstants.ICEBERG_SNAPSHOT_ID_PROPERTY_KEY;
import static org.apache.asterix.external.util.iceberg.IcebergConstants.ICEBERG_SNAPSHOT_TIMESTAMP_PROPERTY_KEY;
import static org.apache.asterix.external.util.iceberg.IcebergConstants.ICEBERG_TABLE_FORMAT;
import static org.apache.asterix.external.util.iceberg.IcebergConstants.ICEBERG_WAREHOUSE_PROPERTY_KEY;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import org.apache.asterix.common.config.CatalogConfig;
import org.apache.asterix.common.config.CatalogConfig.IcebergCatalogSource;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.awsclient.EnsureCloseAWSClientFactory;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.external.util.aws.EnsureCloseClientsFactoryRegistry;
import org.apache.asterix.external.util.aws.iceberg.glue.GlueUtils;
import org.apache.asterix.external.util.google.iceberg.biglake_metastore.BiglakeMetastoreUtils;
import org.apache.asterix.external.util.google.iceberg.fileio.GCSFileIO;
import org.apache.asterix.external.util.iceberg.nessie.NessieUtils;
import org.apache.asterix.external.util.iceberg.rest.RestUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.nessie.NessieCatalog;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class IcebergUtils {

    private static final Logger LOGGER = LogManager.getLogger();

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
        validatePropertyExists(properties, IcebergConstants.ICEBERG_SOURCE_PROPERTY_KEY, ErrorCode.PARAMETERS_REQUIRED);

        String catalogSource = properties.get(IcebergConstants.ICEBERG_SOURCE_PROPERTY_KEY);
        validateAndGetCatalogSource(catalogSource);
        validateCatalogSpecificProperties(properties, catalogSource);
    }

    /**
     * Checks if the provided iceberg catalog source is a supported iceberg catalog type
     *
     * @param catalogSource catalog source
     * @throws CompilationException CompilationException
     */
    public static IcebergCatalogSource validateAndGetCatalogSource(String catalogSource) throws CompilationException {
        for (IcebergCatalogSource icebergSource : IcebergCatalogSource.values()) {
            if (icebergSource.name().equalsIgnoreCase(catalogSource)) {
                return icebergSource;
            }
        }
        throw new CompilationException(ErrorCode.UNSUPPORTED_ICEBERG_CATALOG_SOURCE, catalogSource);
    }

    private static void validateCatalogSpecificProperties(Map<String, String> properties, String catalogSource)
            throws CompilationException {
        Optional<IcebergCatalogSource> source = CatalogConfig.getIcebergCatalogSource(catalogSource);
        if (source.isEmpty()) {
            throw CompilationException.create(ErrorCode.UNSUPPORTED_ICEBERG_CATALOG_SOURCE, catalogSource);
        }

        switch (source.get()) {
            case REST:
                RestUtils.validateRequiredProperties(properties);
                break;
            case AWS_GLUE:
                break;
            case AWS_GLUE_REST:
                GlueUtils.validateGlueRestRequiredProperties(properties);
                break;
            case S3_TABLES:
                GlueUtils.validateS3TablesRequiredProperties(properties);
                break;
            case BIGLAKE_METASTORE:
                BiglakeMetastoreUtils.validateRequiredProperties(properties);
                break;
            case NESSIE:
                NessieUtils.validateRequiredProperties(properties);
                break;
            case NESSIE_REST:
                NessieUtils.validateNessieRestRequiredProperties(properties);
                break;
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
     * Also, prefixes the collection auths with ICEBERG_COLLECTION_PROPERTY_PREFIX_INTERNAL to avoid conflicts
     *
     * @param configuration configuration
     * @return catalog properties
     */
    public static Map<String, String> filterCatalogProperties(Map<String, String> configuration) {
        Map<String, String> properties = new HashMap<>();
        String ioReader = configuration.get(ExternalDataConstants.KEY_EXTERNAL_SOURCE_TYPE);

        for (Map.Entry<String, String> entry : configuration.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(ICEBERG_CATALOG_PROPERTY_PREFIX_INTERNAL)) {
                properties.put(key.substring(ICEBERG_CATALOG_PROPERTY_PREFIX_INTERNAL.length()), entry.getValue());
            } else if (IcebergConstants.authParams.contains(key)) {
                properties.put(ICEBERG_COLLECTION_PROPERTY_PREFIX_INTERNAL + key, entry.getValue());
            }
        }

        // we only need reader type from collection properties (other than auth params) for setting FileIO later
        properties.put(IcebergConstants.ICEBERG_IO_READER_TYPE, ioReader);
        return properties;
    }

    /**
     * Extracts and returns the iceberg catalog properties from the provided configuration
     * Also, prefixes the collection auths with ICEBERG_COLLECTION_PROPERTY_PREFIX_INTERNAL to avoid conflicts
     *
     * @param configuration configuration
     * @return catalog properties
     */
    public static Map<String, String> filterCollectionProperties(Map<String, String> configuration) {
        Map<String, String> properties = new HashMap<>();
        for (Map.Entry<String, String> entry : configuration.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(ICEBERG_COLLECTION_PROPERTY_PREFIX_INTERNAL)) {
                properties.put(key.substring(ICEBERG_COLLECTION_PROPERTY_PREFIX_INTERNAL.length()), entry.getValue());
            }
        }
        return properties;
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
                ICEBERG_CATALOG_PROPERTY_PREFIX_INTERNAL + IcebergConstants.ICEBERG_NAMESPACE_PROPERTY_KEY;
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

    public static Catalog initializeCatalogOnly(Map<String, String> catalogProperties) throws CompilationException {
        return initializeCatalog(catalogProperties, null, false);
    }

    public static Catalog initializeCatalog(Map<String, String> catalogProperties, String namespace)
            throws CompilationException {
        return initializeCatalog(catalogProperties, namespace, true);
    }

    public static Catalog initializeCatalog(Map<String, String> catalogProperties, String namespace,
            boolean initCatalogIo) throws CompilationException {
        // add a hook to close any created clients when the catalog is closed
        String factoryId = UUID.randomUUID().toString();
        catalogProperties.put(EnsureCloseClientsFactoryRegistry.FACTORY_INSTANCE_ID_KEY, factoryId);

        String source = catalogProperties.get(IcebergConstants.ICEBERG_SOURCE_PROPERTY_KEY);
        IcebergCatalogSource catalogSource = validateAndGetCatalogSource(source);

        // remove null values to avoid failures in internal checks
        Catalog catalog;
        catalogProperties.values().removeIf(Objects::isNull);

        try {
            catalog = createAndSetCatalogProperties(catalogProperties, catalogSource);
            setWarehouseIfPresent(catalogProperties);
            if (initCatalogIo) {
                setFileIoProperties(catalogProperties, catalogSource);
            }
            initCatalog(catalog, catalogProperties);
            if (initCatalogIo) {
                validateNamespacePresence((SupportsNamespaces) catalog, namespace);
            }
        } catch (CompilationException ex) {
            throw ex;
        } catch (Throwable ex) {
            throw CompilationException.create(ErrorCode.EXTERNAL_SOURCE_ERROR, ex, ex.getMessage());
        }
        return catalog;
    }

    private static Catalog createAndSetCatalogProperties(Map<String, String> properties, IcebergCatalogSource source)
            throws CompilationException {
        Catalog catalog;
        switch (source) {
            case REST:
            case NESSIE_REST:
                catalog = new RESTCatalog();
                RestUtils.setCatalogProperties(properties);
                break;
            case AWS_GLUE:
                catalog = new GlueCatalog();
                GlueUtils.setGlueCatalogProperties(properties);
                break;
            case AWS_GLUE_REST:
            case S3_TABLES:
                catalog = new RESTCatalog();
                GlueUtils.setGlueRestCatalogProperties(properties);
                break;
            case BIGLAKE_METASTORE:
                catalog = new RESTCatalog();
                BiglakeMetastoreUtils.setCatalogProperties(properties);
                break;
            case NESSIE:
                catalog = new NessieCatalog();
                NessieUtils.setNessieCatalogProperties(properties);
                break;
            default:
                throw CompilationException.create(ErrorCode.UNSUPPORTED_ICEBERG_CATALOG_SOURCE, source);
        }
        return catalog;
    }

    private static void initCatalog(Catalog catalog, Map<String, String> properties) {
        String catalogName = UUID.randomUUID().toString();
        catalog.initialize(catalogName, properties);
        LOGGER.debug("Initialized catalog: {}", catalogName);
    }

    private static void validateNamespacePresence(SupportsNamespaces catalog, String namespace)
            throws CompilationException {
        if (namespace != null && !catalog.namespaceExists(Namespace.of(namespace))) {
            throw CompilationException.create(ErrorCode.ICEBERG_NAMESPACE_DOES_NOT_EXIST, namespace);
        }
    }

    public static void closeAndCleanup(Catalog catalog, Map<String, String> catalogProperties)
            throws CompilationException {
        try {
            if (catalog instanceof AutoCloseable) {
                ((AutoCloseable) catalog).close();
            }
        } catch (Exception ex) {
            throw CompilationException.create(ErrorCode.EXTERNAL_SOURCE_ERROR, ex, ex.getMessage());
        } finally {
            String awsClientsFactoryId = catalogProperties.get(FACTORY_INSTANCE_ID_KEY);
            EnsureCloseClientsFactoryRegistry.closeAll(awsClientsFactoryId);
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

    public static void validatePropertyExists(Map<String, String> properties, String property, ErrorCode errorCode)
            throws CompilationException {
        if (properties.get(property) == null) {
            throw CompilationException.create(errorCode, property);
        }
    }

    public static void setWarehouseIfPresent(Map<String, String> catalogProperties) {
        String warehouse = catalogProperties.get(ICEBERG_WAREHOUSE_PROPERTY_KEY);
        if (warehouse != null && !warehouse.isEmpty()) {
            catalogProperties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);
        }
    }

    public static void setFileIoProperties(Map<String, String> catalogProperties, IcebergCatalogSource catalogSource) {
        if (catalogSource == IcebergCatalogSource.NESSIE_REST) {
            // NESSIE_REST should not set any FileIO properties, it is provided by Nessie server
            return;
        }

        String ioType = catalogProperties.get(IcebergConstants.ICEBERG_IO_READER_TYPE);
        if (ioType == null) {
            throw new IllegalStateException("Iceberg IO reader type is not set");
        }
        if (ioType.equalsIgnoreCase(ExternalDataConstants.KEY_ADAPTER_NAME_AWS_S3)) {
            setIcebergS3FileIoProperties(catalogProperties);
        } else if (ioType.equalsIgnoreCase(ExternalDataConstants.KEY_ADAPTER_NAME_GCS)) {
            setIcebergGcsFileIoProperties(catalogProperties);
        }
    }

    public static void setIcebergS3FileIoProperties(Map<String, String> properties) {
        properties.put(CatalogProperties.FILE_IO_IMPL, IcebergConstants.Aws.S3_FILE_IO);
        properties.put(AwsProperties.CLIENT_FACTORY, EnsureCloseAWSClientFactory.class.getName());
    }

    public static void setIcebergGcsFileIoProperties(Map<String, String> properties) {
        properties.put(CatalogProperties.FILE_IO_IMPL, GCSFileIO.class.getName());
    }

    public static void putCatalogProperties(Map<String, String> addTo, Map<String, String> toAdd) {
        for (Map.Entry<String, String> entry : toAdd.entrySet()) {
            addTo.putIfAbsent(ICEBERG_CATALOG_PROPERTY_PREFIX_INTERNAL + entry.getKey(), entry.getValue());
        }
    }
}
