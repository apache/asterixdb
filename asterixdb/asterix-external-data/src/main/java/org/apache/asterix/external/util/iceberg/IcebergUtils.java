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

import static org.apache.asterix.common.exceptions.ErrorCode.EXTERNAL_SOURCE_ERROR;
import static org.apache.asterix.common.exceptions.ErrorCode.UNSUPPORTED_ICEBERG_DATA_FORMAT;
import static org.apache.asterix.external.util.aws.EnsureCloseClientsFactoryRegistry.FACTORY_INSTANCE_ID_KEY;
import static org.apache.asterix.external.util.iceberg.IcebergConstants.ICEBERG_AVRO_FORMAT;
import static org.apache.asterix.external.util.iceberg.IcebergConstants.ICEBERG_CATALOG_PROPERTY_PREFIX_INTERNAL;
import static org.apache.asterix.external.util.iceberg.IcebergConstants.ICEBERG_COLLECTION_PROPERTY_PREFIX_INTERNAL;
import static org.apache.asterix.external.util.iceberg.IcebergConstants.ICEBERG_PARQUET_FORMAT;
import static org.apache.asterix.external.util.iceberg.IcebergConstants.ICEBERG_TABLE_FORMAT;
import static org.apache.asterix.external.util.iceberg.IcebergConstants.ICEBERG_WAREHOUSE_PROPERTY_KEY;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
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
import org.apache.asterix.external.util.azure.blob.BlobUtils;
import org.apache.asterix.external.util.azure.datalake.DatalakeUtils;
import org.apache.asterix.external.util.google.iceberg.biglake_metastore.BiglakeMetastoreUtils;
import org.apache.asterix.external.util.iceberg.nessie.NessieUtils;
import org.apache.asterix.external.util.iceberg.rest.RestUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.gcp.gcs.GCSFileIO;
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

    /**
     * Whether credential vending is enabled on the catalog behind this configuration -- and therefore whether
     * a collection of that catalog takes its storage credentials from it. Independent of the credentials used
     * to reach the catalog itself.
     *
     * <p>Nothing is recorded on the collection: the catalog's properties are merged in afresh on every
     * compilation, so the switch is simply read where it is needed. The setting appears unprefixed while the
     * catalog's own properties are being validated, and prefixed once they have been merged into a
     * collection, so both spellings are accepted.
     *
     * @param configuration catalog properties, or a collection configuration with them merged in
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Reads the catalog-level vending switch in either property space")
    public static boolean isVendedCredentials(Map<String, String> configuration) {
        String prefixed = configuration.get(
                ICEBERG_CATALOG_PROPERTY_PREFIX_INTERNAL + IcebergConstants.ICEBERG_VENDED_CREDENTIALS_PROPERTY_KEY);
        return Boolean.parseBoolean(prefixed != null ? prefixed
                : configuration.get(IcebergConstants.ICEBERG_VENDED_CREDENTIALS_PROPERTY_KEY));
    }

    /**
     * Whether a catalog of the given source can be asked to vend credentials. This is a structural check
     * only: credential vending is part of the Iceberg REST protocol, so a non-REST source can never offer it.
     * Whether a given <em>endpoint</em> actually vends is a runtime property we cannot decide here, because
     * the endpoint is supplied by the user -- {@code S3_TABLES} is the illustration. Its own endpoint
     * ({@code s3tables.<region>.amazonaws.com}) never vends: it has no notion of Lake Formation and
     * authorizes purely through {@code s3tables:*} IAM actions. The same tables reached through Glue
     * ({@code glue.<region>.amazonaws.com}, warehouse {@code <account>:s3tablescatalog/<bucket>}) do vend,
     * once the bucket is registered with {@code lakeformation register-resource --with-federation}. Both are
     * legal here, so both are allowed, and a non-vending endpoint surfaces at scan time.
     *
     * @param catalogSource catalog source
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI,
            contributionKind = AiProvenance.ContributionKind.GENERATED,
            notes = "Identifies which catalog sources are REST-backed and can therefore vend credentials")
    public static boolean supportsVendedCredentials(IcebergCatalogSource catalogSource) {
        // kept exhaustive on purpose, so a newly added source has to make this choice explicitly
        return switch (catalogSource) {
            case REST, NESSIE_REST, AWS_GLUE_REST, S3_TABLES, BIGLAKE_METASTORE -> true;
            case AWS_GLUE, NESSIE -> false;
        };
    }

    /**
     * Rejects a collection that sets the catalog's vending property itself. Vending is enabled on the catalog
     * and inherited, so the property has no meaning here -- and silently ignoring it would let a user believe
     * they had turned vending on.
     *
     * @param configuration collection properties, before the catalog's are merged in
     * @param sourceLoc     location of the declaration, for error reporting
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Rejects the catalog-level vending property when set on a collection")
    public static void validateVendedCredentialsNotSetOnCollection(Map<String, String> configuration,
            SourceLocation sourceLoc) throws CompilationException {
        if (configuration.containsKey(IcebergConstants.ICEBERG_VENDED_CREDENTIALS_PROPERTY_KEY)) {
            throw new CompilationException(ErrorCode.INVALID_PARAM, sourceLoc,
                    IcebergConstants.ICEBERG_VENDED_CREDENTIALS_PROPERTY_KEY);
        }
    }

    /**
     * Rejects a catalog that enables vending on a source incapable of it. Checked where the claim is made
     * rather than on the first collection to use it, so the error names the thing that is wrong.
     *
     * @param properties the catalog's own properties, unprefixed
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Validates the catalog-level vending switch against the source's capability")
    public static void validateVendedCredentialsCapability(Map<String, String> properties) throws CompilationException {
        if (!isVendedCredentials(properties)) {
            return;
        }
        String catalogSource = properties.get(IcebergConstants.ICEBERG_SOURCE_PROPERTY_KEY);
        if (!supportsVendedCredentials(validateAndGetCatalogSource(catalogSource))) {
            throw new CompilationException(ErrorCode.VENDED_CREDENTIALS_UNSUPPORTED_CATALOG_SOURCE, catalogSource);
        }
    }

    public static void validateCatalogProperties(Map<String, String> properties) throws CompilationException {
        validatePropertyExists(properties, IcebergConstants.ICEBERG_SOURCE_PROPERTY_KEY, ErrorCode.PARAMETERS_REQUIRED);

        String catalogSource = properties.get(IcebergConstants.ICEBERG_SOURCE_PROPERTY_KEY);
        validateAndGetCatalogSource(catalogSource);
        validateVendedCredentialsCapability(properties);
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

        // if timezone is provided, validate it
        String timezone = properties.get(ExternalDataConstants.KEY_TIMEZONE);
        if (timezone != null && !timezone.isEmpty()) {
            ExternalDataUtils.resolveTimeZone(timezone);
        }

        // if variantDepth is provided, it must be an integer in [1, MAX_VARIANT_DEPTH]
        String variantDepth = properties.get(ExternalDataConstants.IcebergOptions.VARIANT_DEPTH);
        if (variantDepth != null && !variantDepth.isEmpty()) {
            validateIntegerInRange(ExternalDataConstants.IcebergOptions.VARIANT_DEPTH, variantDepth, 1,
                    ExternalDataConstants.IcebergOptions.MAX_VARIANT_DEPTH);
        }

        // if either pushdown flag is provided, it must be a boolean
        validateBoolean(properties, ExternalDataConstants.IcebergOptions.VARIANT_PROJECTION_PUSHDOWN);
        validateBoolean(properties, ExternalDataConstants.IcebergOptions.VARIANT_STATS_PUSHDOWN);
        validateBoolean(properties, ExternalDataConstants.IcebergOptions.VARIANT_PROJECTION_PUSHDOWN_WITH_DELETES);

        // validate snapshot
        IcebergSnapshotUtils.validateAndGetSnapshot(properties);
    }

    /**
     * Rejects a non-boolean value for an optional boolean WITH-clause option.
     * <p>
     * Worth validating because {@code Boolean.parseBoolean} maps anything it does not recognise to {@code false}, so a
     * typo like {@code "ture"} or {@code "1"} would silently <em>disable</em> the optimization instead of failing.
     * <p>
     * Empty is rejected too, unlike {@code variantDepth} which treats it as absent: that option's runtime read falls
     * back to the default on an empty value, while these flags are read with {@code getOrDefault} and so would see
     * {@code ""}, which {@code parseBoolean} reads as off.
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "DDL-time validation for the variant pushdown boolean flags, following the variantDepth pattern; rejects empty as well since parseBoolean would read it as a silent off")
    private static void validateBoolean(Map<String, String> properties, String propertyName)
            throws CompilationException {
        String value = properties.get(propertyName);
        if (value == null) {
            return;
        }
        if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
            throw new CompilationException(ErrorCode.INVALID_REQ_PARAM_VAL, propertyName, value);
        }
    }

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_SONNET_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Validates the variantDepth WITH-clause option at DDL time, matching the existing timezone validation pattern in this method. The catch block is reserved purely for genuine Integer.parseInt failures; the out-of-range case throws CompilationException directly")
    private static void validateIntegerInRange(String propertyName, String value, int min, int max)
            throws CompilationException {
        int parsed;
        try {
            parsed = Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new CompilationException(ErrorCode.INVALID_REQ_PARAM_VAL, propertyName, value);
        }
        if (parsed < min || parsed > max) {
            throw new CompilationException(ErrorCode.INVALID_REQ_PARAM_VAL, propertyName, value);
        }
    }

    /**
     * Parses a dot-separated namespace string into an Iceberg {@link Namespace}.
     * For example, "namespace.subnamespace" returns Namespace.of("namespace", "subnamespace").
     *
     * @param namespace dot-separated namespace string
     * @return Iceberg Namespace
     */
    public static Namespace parseNamespace(String namespace) {
        return Namespace.of(namespace.split("\\."));
    }

    /**
     * Extracts and returns the iceberg catalog properties from the provided configuration
     * Also, prefixes the collection auths with ICEBERG_COLLECTION_PROPERTY_PREFIX_INTERNAL to avoid conflicts
     *
     * @param configuration configuration
     * @return catalog properties
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED, notes = "Carry the vended-credentials marker through to the catalog properties so setFileIoProperties can see it")
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
        // setFileIoProperties reads the vending switch from these properties; it arrives via the
        // catalog-prefix branch above, so nothing further needs carrying across.
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
            setAccessDelegationIfVended(catalogProperties);
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
        if (namespace != null && !catalog.namespaceExists(IcebergUtils.parseNamespace(namespace))) {
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
            if (catalogProperties != null) {
                String awsClientsFactoryId = catalogProperties.get(FACTORY_INSTANCE_ID_KEY);
                EnsureCloseClientsFactoryRegistry.closeAll(awsClientsFactoryId);
            }
        }
    }

    public static String[] getProjectedFields(Map<String, String> configuration) throws IOException {
        String encoded = configuration.get(ExternalDataConstants.KEY_REQUESTED_FIELDS);
        ARecordType projectedRecordType = ExternalDataUtils.getExpectedType(encoded);
        return projectedRecordType.getFieldNames();
    }

    public static void setDefaults(Map<String, String> configuration) {
        setDefaultFormat(configuration);
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

    public static void validateIcebergCatalogUri(Map<String, String> properties) throws CompilationException {
        String value = properties.get(IcebergConstants.ICEBERG_URI_PROPERTY_KEY);
        if (value == null) {
            return;
        }
        if (value.isBlank()) {
            throw CompilationException.create(EXTERNAL_SOURCE_ERROR, "received blank URI value");
        }

        try {
            URI uri = new URI(value);
            if (!uri.isAbsolute()) {
                throw CompilationException.create(EXTERNAL_SOURCE_ERROR, "missing URI scheme. received: " + value);
            }

            if (uri.getHost() == null || uri.getHost().isBlank()) {
                throw CompilationException.create(EXTERNAL_SOURCE_ERROR,
                        "URI host cannot be blank. received: " + value);
            }
        } catch (URISyntaxException ex) {
            throw CompilationException.create(EXTERNAL_SOURCE_ERROR, ex, ex.toString());
        }
    }

    public static void setWarehouseIfPresent(Map<String, String> catalogProperties) {
        String warehouse = catalogProperties.get(ICEBERG_WAREHOUSE_PROPERTY_KEY);
        if (warehouse != null && !warehouse.isEmpty()) {
            catalogProperties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);
        }
    }

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.ASSISTED, notes = "Skip FileIO configuration when the catalog vends the credentials")
    /**
     * Asks the catalog to vend storage credentials for the tables it hands out. Only the request side is ours:
     * the credentials come back on the load-table response and the Iceberg client applies them to the table's
     * FileIO itself, which is also why {@link #setFileIoProperties} must not configure one.
     *
     * @param catalogProperties catalog properties
     */
    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.GENERATED, notes = "Requests credential vending via the REST access-delegation header")
    private static void setAccessDelegationIfVended(Map<String, String> catalogProperties) {
        if (isVendedCredentials(catalogProperties)) {
            catalogProperties.put(IcebergConstants.Rest.ACCESS_DELEGATION_HEADER_PROPERTY,
                    IcebergConstants.Rest.ACCESS_DELEGATION_VENDED_CREDENTIALS);
        }
    }

    public static void setFileIoProperties(Map<String, String> catalogProperties, IcebergCatalogSource catalogSource)
            throws CompilationException {
        if (catalogSource == IcebergCatalogSource.NESSIE_REST) {
            // NESSIE_REST should not set any FileIO properties, it is provided by Nessie server
            return;
        }

        if (isVendedCredentials(catalogProperties)) {
            // the catalog vends the storage credentials and supplies the FileIO configuration along with them,
            // so configuring one here would override what is vended
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
        } else if (BlobUtils.isBlobAdapter(ioType) || DatalakeUtils.isDatalakeAdapter(ioType)) {
            // ADLSFileIO is used for both Blob storage and Datalake storage
            setIcebergAzureAdlsFileIoProperties(catalogProperties);
        } else {
            throw CompilationException.create(ErrorCode.UNSUPPORTED_FILE_IO_TYPE, ioType);
        }
    }

    public static void setIcebergS3FileIoProperties(Map<String, String> properties) {
        properties.put(CatalogProperties.FILE_IO_IMPL, IcebergConstants.Aws.S3_FILE_IO);
        properties.put(AwsProperties.CLIENT_FACTORY, EnsureCloseAWSClientFactory.class.getName());
    }

    public static void setIcebergGcsFileIoProperties(Map<String, String> properties) {
        properties.put(CatalogProperties.FILE_IO_IMPL, GCSFileIO.class.getName());
    }

    public static void setIcebergAzureAdlsFileIoProperties(Map<String, String> properties) throws CompilationException {
        properties.put(CatalogProperties.FILE_IO_IMPL, IcebergConstants.Azure.ADLS_FILE_IO);
        DatalakeUtils.setIcebergAdlsAuthParams(properties);
    }

    public static void putCatalogProperties(Map<String, String> addTo, Map<String, String> toAdd) {
        for (Map.Entry<String, String> entry : toAdd.entrySet()) {
            addTo.putIfAbsent(ICEBERG_CATALOG_PROPERTY_PREFIX_INTERNAL + entry.getKey(), entry.getValue());
        }
    }

    public static List<Namespace> listNamespaces(Catalog catalog, CatalogConfig.IcebergCatalogSource source) {
        return switch (source) {
            case AWS_GLUE -> {
                GlueCatalog glueCatalog = (GlueCatalog) catalog;
                yield glueCatalog.listNamespaces();
            }
            case REST, AWS_GLUE_REST, BIGLAKE_METASTORE, S3_TABLES, NESSIE_REST -> {
                RESTCatalog restCatalog = (RESTCatalog) catalog;
                yield restCatalog.listNamespaces();
            }
            case NESSIE -> {
                NessieCatalog nessieCatalog = (NessieCatalog) catalog;
                yield nessieCatalog.listNamespaces();
            }
        };
    }

    public static List<TableIdentifier> listTables(Catalog catalog, Namespace namespace, CatalogConfig.IcebergCatalogSource source) {
        return switch (source) {
            case AWS_GLUE -> {
                GlueCatalog glueCatalog = (GlueCatalog) catalog;
                yield glueCatalog.listTables(namespace);
            }
            case REST, AWS_GLUE_REST, BIGLAKE_METASTORE, S3_TABLES, NESSIE_REST -> {
                RESTCatalog restCatalog = (RESTCatalog) catalog;
                yield restCatalog.listTables(namespace);
            }
            case NESSIE -> {
                NessieCatalog nessieCatalog = (NessieCatalog) catalog;
                yield nessieCatalog.listTables(namespace);
            }
        };
    }

    public static Table loadTable(Catalog catalog, TableIdentifier tableIdentifier, CatalogConfig.IcebergCatalogSource source) {
        return switch (source) {
            case AWS_GLUE -> {
                GlueCatalog glueCatalog = (GlueCatalog) catalog;
                yield glueCatalog.loadTable(tableIdentifier);
            }
            case REST, AWS_GLUE_REST, BIGLAKE_METASTORE, S3_TABLES, NESSIE_REST -> {
                RESTCatalog restCatalog = (RESTCatalog) catalog;
                yield restCatalog.loadTable(tableIdentifier);
            }
            case NESSIE -> {
                NessieCatalog nessieCatalog = (NessieCatalog) catalog;
                yield nessieCatalog.loadTable(tableIdentifier);
            }
        };
    }
}
