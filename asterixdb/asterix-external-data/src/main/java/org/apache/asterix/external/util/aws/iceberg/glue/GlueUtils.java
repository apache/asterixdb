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
package org.apache.asterix.external.util.aws.iceberg.glue;

import static org.apache.asterix.common.exceptions.ErrorCode.PARAMETERS_REQUIRED;
import static org.apache.asterix.external.util.aws.AwsConstants.REGION_FIELD_NAME;
import static org.apache.asterix.external.util.aws.AwsConstants.SERVICE_END_POINT_FIELD_NAME;
import static org.apache.asterix.external.util.aws.AwsUtils.buildCredentialsProvider;
import static org.apache.asterix.external.util.aws.AwsUtils.validateAndGetRegion;
import static org.apache.asterix.external.util.iceberg.IcebergConstants.ICEBERG_SOURCE_PROPERTY_KEY;
import static org.apache.asterix.external.util.iceberg.IcebergConstants.ICEBERG_URI_PROPERTY_KEY;
import static org.apache.asterix.external.util.iceberg.IcebergConstants.ICEBERG_WAREHOUSE_PROPERTY_KEY;
import static org.apache.asterix.external.util.iceberg.IcebergConstants.Aws.REST_SIG4_SIGNING_NAME;
import static org.apache.asterix.external.util.iceberg.IcebergConstants.Aws.REST_SIG4_SIGNING_NAME_PROPERTY_NAME;
import static org.apache.asterix.external.util.iceberg.IcebergConstants.Aws.REST_SIG4_SIGNING_REGION;
import static org.apache.asterix.external.util.iceberg.IcebergConstants.Aws.REST_SIG4_SIGNING_REGION_PROPERTY_NAME;
import static org.apache.asterix.external.util.iceberg.IcebergUtils.validatePropertyExists;
import static org.apache.iceberg.CatalogProperties.CATALOG_IMPL;
import static org.apache.iceberg.CatalogProperties.URI;
import static org.apache.iceberg.aws.AwsProperties.CLIENT_FACTORY;
import static org.apache.iceberg.rest.auth.AuthProperties.AUTH_TYPE;

import java.util.Map;

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.config.CatalogConfig;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.external.awsclient.EnsureCloseAWSClientFactory;
import org.apache.asterix.external.util.aws.AwsUtils;
import org.apache.asterix.external.util.aws.AwsUtils.CloseableAwsClients;
import org.apache.asterix.external.util.aws.iceberg.auth.EnsureCloseClientsRESTSigV4AuthManager;
import org.apache.asterix.external.util.iceberg.IcebergConstants;
import org.apache.asterix.external.util.iceberg.IcebergUtils;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.rest.RESTCatalog;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.GlueClientBuilder;

public class GlueUtils {

    private GlueUtils() {
        throw new AssertionError("do not instantiate");
    }

    /**
     * Builds the client using the provided configuration
     *
     * @param configuration properties
     * @return client
     * @throws CompilationException CompilationException
     */
    public static CloseableAwsClients buildClient(IApplicationContext appCtx, Map<String, String> configuration)
            throws CompilationException {
        CloseableAwsClients awsClients = new CloseableAwsClients();
        String regionId = configuration.get(REGION_FIELD_NAME);
        String serviceEndpoint = configuration.get(SERVICE_END_POINT_FIELD_NAME);

        Region region = validateAndGetRegion(regionId);
        AwsCredentialsProvider credentialsProvider = buildCredentialsProvider(appCtx, configuration, awsClients);

        GlueClientBuilder builder = GlueClient.builder();
        builder.region(region);
        builder.credentialsProvider(credentialsProvider);
        AwsUtils.setEndpoint(builder, serviceEndpoint);

        awsClients.setConsumingClient(builder.build());
        return awsClients;
    }

    public static void setGlueCatalogProperties(Map<String, String> catalogProperties) {
        catalogProperties.put(CatalogProperties.CATALOG_IMPL, GlueCatalog.class.getName());
        catalogProperties.put(CLIENT_FACTORY, EnsureCloseAWSClientFactory.class.getName());
    }

    public static void setGlueRestCatalogProperties(Map<String, String> catalogProperties) throws CompilationException {
        catalogProperties.put(CATALOG_IMPL, RESTCatalog.class.getName());
        catalogProperties.put(URI, catalogProperties.get(ICEBERG_URI_PROPERTY_KEY));
        catalogProperties.put(AUTH_TYPE, EnsureCloseClientsRESTSigV4AuthManager.class.getName());
        catalogProperties.put(REST_SIG4_SIGNING_NAME, getSigningName(catalogProperties));
        catalogProperties.put(REST_SIG4_SIGNING_REGION, catalogProperties.get(REST_SIG4_SIGNING_REGION_PROPERTY_NAME));
        catalogProperties.put(CLIENT_FACTORY, EnsureCloseAWSClientFactory.class.getName());
    }

    private static String getSigningName(Map<String, String> catalogProperties) throws CompilationException {
        String catalogSource = catalogProperties.get(ICEBERG_SOURCE_PROPERTY_KEY);
        CatalogConfig.IcebergCatalogSource source = IcebergUtils.validateAndGetCatalogSource(catalogSource);
        if (source == CatalogConfig.IcebergCatalogSource.AWS_GLUE_REST) {
            return IcebergConstants.Aws.REST_SIG4_GLUE_SIGNING_NAME;
        } else {
            return catalogProperties.get(REST_SIG4_SIGNING_NAME_PROPERTY_NAME);
        }
    }

    public static void validateGlueRestRequiredProperties(Map<String, String> catalogProperties)
            throws CompilationException {
        validatePropertyExists(catalogProperties, ICEBERG_URI_PROPERTY_KEY, PARAMETERS_REQUIRED);
        validatePropertyExists(catalogProperties, REST_SIG4_SIGNING_REGION_PROPERTY_NAME, PARAMETERS_REQUIRED);
    }

    public static void validateS3TablesRequiredProperties(Map<String, String> catalogProperties)
            throws CompilationException {
        validatePropertyExists(catalogProperties, ICEBERG_WAREHOUSE_PROPERTY_KEY, PARAMETERS_REQUIRED);
        validatePropertyExists(catalogProperties, ICEBERG_URI_PROPERTY_KEY, PARAMETERS_REQUIRED);
        validatePropertyExists(catalogProperties, REST_SIG4_SIGNING_NAME_PROPERTY_NAME, PARAMETERS_REQUIRED);
        validatePropertyExists(catalogProperties, REST_SIG4_SIGNING_REGION_PROPERTY_NAME, PARAMETERS_REQUIRED);
    }
}
