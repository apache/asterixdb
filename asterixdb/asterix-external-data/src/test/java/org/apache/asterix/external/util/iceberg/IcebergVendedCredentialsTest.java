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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.config.CatalogConfig.IcebergCatalogSource;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.aws.iceberg.glue.GlueUtils;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.gcp.gcs.GCSFileIO;
import org.junit.Test;

/**
 * Compile-time and validation-time behaviour of Iceberg credential vending. Nothing here reads data or
 * contacts a catalog; the point is the decisions made before any I/O happens.
 *
 * <p>Every "X is not configured" assertion is paired with a control showing that X <em>is</em> configured
 * when credentials are not vended — otherwise the assertion would pass just as happily if the feature were
 * deleted.
 */
public class IcebergVendedCredentialsTest {

    private static final String SWITCH = IcebergConstants.ICEBERG_VENDED_CREDENTIALS_PROPERTY_KEY;

    /**
     * Every storage type the FileIO dispatch knows, with the FileIO each configures when credentials are not
     * vended. When they are vended none of these is configured, and Iceberg's ResolvingFileIO picks the
     * implementation from the data file's location scheme instead -- s3, gs or abfss.
     */
    private static final Map<String, String> STORAGE_FILE_IO = Map.of(ExternalDataConstants.KEY_ADAPTER_NAME_AWS_S3,
            IcebergConstants.Aws.S3_FILE_IO, ExternalDataConstants.KEY_ADAPTER_NAME_GCS, GCSFileIO.class.getName(),
            ExternalDataConstants.KEY_ADAPTER_NAME_AZURE_DATALAKE, IcebergConstants.Azure.ADLS_FILE_IO,
            ExternalDataConstants.KEY_ADAPTER_NAME_AZURE_BLOB, IcebergConstants.Azure.ADLS_FILE_IO);

    /**
     * Sources that may be asked to vend, i.e. the REST-backed ones. Whether the endpoint behind them really
     * vends is a runtime property -- the user supplies the endpoint -- so it is deliberately not decided here.
     */
    private static final Set<IcebergCatalogSource> CAN_VEND =
            EnumSet.of(IcebergCatalogSource.REST, IcebergCatalogSource.NESSIE_REST, IcebergCatalogSource.AWS_GLUE_REST,
                    IcebergCatalogSource.S3_TABLES, IcebergCatalogSource.BIGLAKE_METASTORE);

    // ------------------------------------------------------------------------------------------------
    // the catalog's vending switch
    // ------------------------------------------------------------------------------------------------

    @Test
    public void switchIsReadAsABoolean() {
        assertFalse("absent must not read as vended", IcebergUtils.isVendedCredentials(new HashMap<>()));
        assertFalse(IcebergUtils.isVendedCredentials(props(SWITCH, "false")));
        assertFalse("a non-boolean must not read as vended", IcebergUtils.isVendedCredentials(props(SWITCH, "yes")));
        assertTrue(IcebergUtils.isVendedCredentials(props(SWITCH, "true")));
        assertTrue("the value is parsed case-insensitively", IcebergUtils.isVendedCredentials(props(SWITCH, "TRUE")));
    }

    // ------------------------------------------------------------------------------------------------
    // which sources can vend
    // ------------------------------------------------------------------------------------------------

    /**
     * Exhaustive on purpose: a newly added catalog source must fail here until someone decides whether it
     * can vend, rather than silently inheriting a default.
     */
    @Test
    public void everySourceIsClassified() {
        for (IcebergCatalogSource source : IcebergCatalogSource.values()) {
            assertEquals("unexpected vending support for " + source, CAN_VEND.contains(source),
                    IcebergUtils.supportsVendedCredentials(source));
        }
        assertEquals("a source was added or removed; classify it above", 7, IcebergCatalogSource.values().length);
    }

    // ------------------------------------------------------------------------------------------------
    // a catalog may only claim vending if its source can do it -- checked where the claim is made
    // ------------------------------------------------------------------------------------------------

    @Test
    public void sourceIsNotCheckedWhenCatalogDoesNotVend() throws CompilationException {
        // control: this source cannot vend, but without the switch the capability must not be consulted
        IcebergUtils.validateVendedCredentialsCapability(catalogProps(IcebergCatalogSource.AWS_GLUE, false));
    }

    @Test
    public void vendingCatalogAcceptedOnRestBackedSources() throws CompilationException {
        for (IcebergCatalogSource source : CAN_VEND) {
            IcebergUtils.validateVendedCredentialsCapability(catalogProps(source, true));
        }
    }

    @Test
    public void vendingCatalogRejectedOnNonRestSources() {
        for (IcebergCatalogSource source : EnumSet.complementOf(EnumSet.copyOf(CAN_VEND))) {
            try {
                IcebergUtils.validateVendedCredentialsCapability(catalogProps(source, true));
                fail("expected " + source + " to be rejected for vending");
            } catch (CompilationException e) {
                assertEquals("unexpected error for " + source,
                        ErrorCode.VENDED_CREDENTIALS_UNSUPPORTED_CATALOG_SOURCE.intValue(), e.getErrorCode());
            }
        }
    }

    // ------------------------------------------------------------------------------------------------
    // a collection may not set the catalog's switch itself
    // ------------------------------------------------------------------------------------------------

    @Test
    public void collectionSettingTheCatalogSwitchIsRejected() {
        try {
            IcebergUtils.validateVendedCredentialsNotSetOnCollection(icebergProps(), null);
            fail("expected a collection-set vending property to be rejected");
        } catch (CompilationException e) {
            assertEquals(ErrorCode.INVALID_PARAM.intValue(), e.getErrorCode());
        }
    }

    @Test
    public void collectionWithoutTheSwitchIsAccepted() throws CompilationException {
        // control for the test above
        Map<String, String> config = icebergProps();
        config.remove(SWITCH);
        IcebergUtils.validateVendedCredentialsNotSetOnCollection(config, null);
    }

    // ------------------------------------------------------------------------------------------------
    // resolution: the switch is read in either property space
    // ------------------------------------------------------------------------------------------------

    /**
     * The switch is spelled unprefixed while the catalog's own properties are being validated, and prefixed
     * once they have been merged into a collection. Both have to resolve, because the same method is called
     * from both points.
     */
    @Test
    public void switchResolvesPrefixedAndUnprefixed() {
        String prefixed = IcebergConstants.ICEBERG_CATALOG_PROPERTY_PREFIX_INTERNAL
                + IcebergConstants.ICEBERG_VENDED_CREDENTIALS_PROPERTY_KEY;

        assertTrue("a merged collection reads the prefixed switch",
                IcebergUtils.isVendedCredentials(props(prefixed, "true")));
        assertTrue("a catalog being validated reads the unprefixed switch",
                IcebergUtils.isVendedCredentials(props(SWITCH, "true")));
        assertFalse(IcebergUtils.isVendedCredentials(props(prefixed, "false")));

        // the prefixed spelling is the merged one, so it is what wins where both appear
        Map<String, String> both = props(prefixed, "true");
        both.put(SWITCH, "false");
        assertTrue("the merged catalog switch decides", IcebergUtils.isVendedCredentials(both));
    }

    // ------------------------------------------------------------------------------------------------
    // the switch has to survive the catalog-property filter
    // ------------------------------------------------------------------------------------------------

    /**
     * {@code filterCatalogProperties} keeps only catalog-prefixed keys and a short allowlist, and the
     * scan-time FileIO setup reads the switch out of what it returns. A switch dropped here stops every
     * check downstream of it from firing, silently. That happened once already.
     */
    @Test
    public void switchSurvivesCatalogPropertyFiltering() {
        Map<String, String> collection =
                props(ExternalDataConstants.KEY_EXTERNAL_SOURCE_TYPE, ExternalDataConstants.KEY_ADAPTER_NAME_AWS_S3);
        collection.put(IcebergConstants.ICEBERG_CATALOG_PROPERTY_PREFIX_INTERNAL
                + IcebergConstants.ICEBERG_VENDED_CREDENTIALS_PROPERTY_KEY, "true");

        assertTrue("the catalog's switch was dropped by filtering",
                IcebergUtils.isVendedCredentials(IcebergUtils.filterCatalogProperties(collection)));

        // control: it is carried, not fabricated. A collection whose catalog does not vend -- which is also
        // what a collection with a link of its own looks like, the switch being withheld from it -- must not
        // come out of filtering as vending.
        Map<String, String> nonVending =
                props(ExternalDataConstants.KEY_EXTERNAL_SOURCE_TYPE, ExternalDataConstants.KEY_ADAPTER_NAME_AWS_S3);
        assertFalse(IcebergUtils.isVendedCredentials(IcebergUtils.filterCatalogProperties(nonVending)));
    }

    // ------------------------------------------------------------------------------------------------
    // no FileIO is configured when the catalog supplies it
    // ------------------------------------------------------------------------------------------------

    /**
     * The skip covers every storage type, not only S3: it happens before the reader-type dispatch, so no
     * storage type gets a FileIO of its own. Each cloud then refreshes the vended credentials through its own
     * provider, keyed off a refresh endpoint the catalog returns -- VendedCredentialsProvider on S3,
     * OAuth2RefreshCredentialsHandler on GCS, VendedAdlsCredentialProvider on ADLS.
     */
    @Test
    public void fileIoIsNotConfiguredWhenVendingForAnyStorageType() throws CompilationException {
        for (String storageType : STORAGE_FILE_IO.keySet()) {
            Map<String, String> catalogProps = ioReaderProps(storageType);
            catalogProps.put(SWITCH, "true");

            IcebergUtils.setFileIoProperties(catalogProps, IcebergCatalogSource.REST);
            assertNull("configuring a FileIO would override what the catalog vended for " + storageType,
                    catalogProps.get(CatalogProperties.FILE_IO_IMPL));
        }
    }

    @Test
    public void fileIoIsConfiguredWhenNotVending() throws CompilationException {
        // control for the test above: without the marker a FileIO is configured as usual. Azure is excluded
        // because its branch does more than name a FileIO -- see azureCredentialsAreNotDemandedWhenVending.
        for (Map.Entry<String, String> storage : STORAGE_FILE_IO.entrySet()) {
            String storageType = storage.getKey();
            if (IcebergConstants.Azure.ADLS_FILE_IO.equals(storage.getValue())) {
                continue;
            }
            Map<String, String> catalogProps = ioReaderProps(storageType);

            IcebergUtils.setFileIoProperties(catalogProps, IcebergCatalogSource.REST);
            assertEquals(storageType, storage.getValue(), catalogProps.get(CatalogProperties.FILE_IO_IMPL));
        }
    }

    /**
     * Azure is the storage type where skipping the FileIO is load-bearing rather than merely tidy. Its branch
     * resolves the collection's own Azure credentials into ADLS auth properties, which a vended-credentials
     * collection does not have -- so reaching it at all fails the DDL. The control below shows it does fail
     * without the marker, which is what makes the assertion above meaningful.
     */
    @Test
    public void azureCredentialsAreNotDemandedWhenVending() throws CompilationException {
        for (String storageType : azureStorageTypes()) {
            Map<String, String> catalogProps = ioReaderProps(storageType);
            catalogProps.put(SWITCH, "true");

            IcebergUtils.setFileIoProperties(catalogProps, IcebergCatalogSource.REST);
            assertNull(storageType, catalogProps.get(CatalogProperties.FILE_IO_IMPL));
        }
    }

    @Test
    public void azureCredentialsAreDemandedWhenNotVending() {
        // control for the test above: the same configuration without the marker cannot resolve Azure auth
        for (String storageType : azureStorageTypes()) {
            try {
                IcebergUtils.setFileIoProperties(ioReaderProps(storageType), IcebergCatalogSource.REST);
                fail("expected " + storageType + " to demand the collection's own Azure credentials");
            } catch (CompilationException e) {
                assertEquals(storageType, ErrorCode.PARAMETERS_REQUIRED.intValue(), e.getErrorCode());
            }
        }
    }

    // ------------------------------------------------------------------------------------------------
    // no storage client factory when the catalog supplies the credentials
    // ------------------------------------------------------------------------------------------------

    /**
     * The Glue REST and S3 Tables catalogs install the client factory in {@code createAndSetCatalogProperties},
     * which runs before FileIO setup — so skipping FileIO does not cover them. Left installed, it would build
     * S3 clients from the collection's own credentials and quietly ignore the vended ones.
     */
    @Test
    public void glueRestClientFactoryIsNotInstalledWhenVending() throws CompilationException {
        Map<String, String> catalogProps = glueRestProps();
        catalogProps.put(SWITCH, "true");

        GlueUtils.setGlueRestCatalogProperties(catalogProps);
        assertNull("the client factory would ignore the vended credentials",
                catalogProps.get(AwsProperties.CLIENT_FACTORY));
        assertEquals("sigv4 reaches the catalog itself and must stay", IcebergConstants.Aws.REST_SIG4_GLUE_SIGNING_NAME,
                catalogProps.get(IcebergConstants.Aws.REST_SIG4_SIGNING_NAME));
    }

    @Test
    public void glueRestClientFactoryIsInstalledWhenNotVending() throws CompilationException {
        // control for the test above
        Map<String, String> catalogProps = glueRestProps();

        GlueUtils.setGlueRestCatalogProperties(catalogProps);
        assertEquals("org.apache.asterix.external.awsclient.EnsureCloseAWSClientFactory",
                catalogProps.get(AwsProperties.CLIENT_FACTORY));
    }

    // ------------------------------------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------------------------------------

    private static Map<String, String> props(String key, String value) {
        Map<String, String> config = new HashMap<>();
        config.put(key, value);
        return config;
    }

    private static Map<String, String> icebergProps() {
        Map<String, String> config = props(ExternalDataConstants.TABLE_FORMAT, IcebergConstants.ICEBERG_TABLE_FORMAT);
        config.put(SWITCH, "true");
        return config;
    }

    private static Map<String, String> sourceProps(IcebergCatalogSource source) {
        Map<String, String> config = icebergProps();
        config.put(IcebergConstants.ICEBERG_CATALOG_PROPERTY_PREFIX_INTERNAL
                + IcebergConstants.ICEBERG_SOURCE_PROPERTY_KEY, source.name());
        return config;
    }

    /** A catalog's own properties as CREATE CATALOG sees them: unprefixed, with the vending switch. */
    private static Map<String, String> catalogProps(IcebergCatalogSource source, boolean vending) {
        Map<String, String> config = props(IcebergConstants.ICEBERG_SOURCE_PROPERTY_KEY, source.name());
        if (vending) {
            config.put(IcebergConstants.ICEBERG_VENDED_CREDENTIALS_PROPERTY_KEY, "true");
        }
        return config;
    }

    private static Map<String, String> ioReaderProps(String storageType) {
        return props(IcebergConstants.ICEBERG_IO_READER_TYPE, storageType);
    }

    private static Set<String> azureStorageTypes() {
        return STORAGE_FILE_IO.entrySet().stream().filter(e -> IcebergConstants.Azure.ADLS_FILE_IO.equals(e.getValue()))
                .map(Map.Entry::getKey).collect(java.util.stream.Collectors.toSet());
    }

    private static Map<String, String> glueRestProps() {
        Map<String, String> config =
                props(IcebergConstants.ICEBERG_URI_PROPERTY_KEY, "https://glue.eu-central-1.amazonaws.com/iceberg");
        config.put(IcebergConstants.ICEBERG_SOURCE_PROPERTY_KEY, IcebergCatalogSource.AWS_GLUE_REST.name());
        config.put(IcebergConstants.Aws.REST_SIG4_SIGNING_REGION_PROPERTY_NAME, "eu-central-1");
        return config;
    }
}
