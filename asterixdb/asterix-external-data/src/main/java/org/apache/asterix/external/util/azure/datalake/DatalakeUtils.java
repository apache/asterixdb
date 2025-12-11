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
package org.apache.asterix.external.util.azure.datalake;

import static org.apache.asterix.common.exceptions.ErrorCode.PARAMETERS_REQUIRED;
import static org.apache.asterix.common.exceptions.ErrorCode.PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT;
import static org.apache.asterix.common.exceptions.ErrorCode.REQUIRED_PARAM_IF_PARAM_IS_PRESENT;
import static org.apache.asterix.external.util.ExternalDataUtils.getFirstNotNull;
import static org.apache.asterix.external.util.ExternalDataUtils.getPrefix;
import static org.apache.asterix.external.util.ExternalDataUtils.isDeltaTable;
import static org.apache.asterix.external.util.ExternalDataUtils.validateDeltaTableProperties;
import static org.apache.asterix.external.util.ExternalDataUtils.validateIncludeExclude;
import static org.apache.asterix.external.util.azure.AzureConstants.ACCOUNT_KEY_FIELD_NAME;
import static org.apache.asterix.external.util.azure.AzureConstants.ACCOUNT_NAME_FIELD_NAME;
import static org.apache.asterix.external.util.azure.AzureConstants.CLIENT_ID_FIELD_NAME;
import static org.apache.asterix.external.util.azure.AzureConstants.CLIENT_SECRET_FIELD_NAME;
import static org.apache.asterix.external.util.azure.AzureConstants.ENDPOINT_FIELD_NAME;
import static org.apache.asterix.external.util.azure.AzureConstants.MANAGED_IDENTITY_FIELD_NAME;
import static org.apache.asterix.external.util.azure.AzureConstants.SHARED_ACCESS_SIGNATURE_FIELD_NAME;
import static org.apache.asterix.external.util.azure.AzureConstants.TENANT_ID_FIELD_NAME;
import static org.apache.asterix.external.util.azure.datalake.DatalakeConstants.DEFAULT_RECUSRIVE_VALUE;
import static org.apache.asterix.external.util.azure.datalake.DatalakeConstants.RECURSIVE_FIELD_NAME;
import static org.apache.hyracks.api.util.ExceptionUtils.getMessageOrToString;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.regex.Matcher;

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.external.IExternalFilterEvaluator;
import org.apache.asterix.external.input.record.reader.abstracts.AbstractExternalInputStreamFactory;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataPrefix;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.external.util.azure.AzureConstants;
import org.apache.asterix.external.util.azure.AzureUtils;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.exceptions.Warning;

import com.azure.core.credential.AzureSasCredential;
import com.azure.core.http.rest.PagedIterable;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceAsyncClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.models.ListPathsOptions;
import com.azure.storage.file.datalake.models.PathItem;

public class DatalakeUtils {
    private DatalakeUtils() {
        throw new AssertionError("do not instantiate");
    }

    public static DataLakeServiceClient buildClient(IApplicationContext appCtx, Map<String, String> configuration)
            throws CompilationException {
        return buildClient(DataLakeServiceClient.class, appCtx, configuration);
    }

    public static DataLakeServiceAsyncClient buildAsyncClient(IApplicationContext appCtx,
            Map<String, String> configuration) throws CompilationException {
        return buildClient(DataLakeServiceAsyncClient.class, appCtx, configuration);
    }

    /**
     * Builds the Azure storage account using the provided configuration
     *
     * @param configuration properties
     * @return client
     */
    private static <T> T buildClient(Class<T> type, IApplicationContext appCtx, Map<String, String> configuration)
            throws CompilationException {
        String managedIdentity = configuration.get(MANAGED_IDENTITY_FIELD_NAME);
        String accountName = configuration.get(ACCOUNT_NAME_FIELD_NAME);
        String accountKey = configuration.get(ACCOUNT_KEY_FIELD_NAME);
        String sharedAccessSignature = configuration.get(SHARED_ACCESS_SIGNATURE_FIELD_NAME);
        String tenantId = configuration.get(TENANT_ID_FIELD_NAME);
        String clientId = configuration.get(CLIENT_ID_FIELD_NAME);
        String clientSecret = configuration.get(CLIENT_SECRET_FIELD_NAME);
        String endpoint = configuration.get(ENDPOINT_FIELD_NAME);

        // Client builder
        DataLakeServiceClientBuilder builder = new DataLakeServiceClientBuilder();
        builder.httpLogOptions(AzureConstants.HTTP_LOG_OPTIONS);

        int timeout = appCtx.getExternalProperties().getAzureRequestTimeout();
        RequestRetryOptions requestRetryOptions = new RequestRetryOptions(null, null, timeout, null, null, null);
        builder.retryOptions(requestRetryOptions);

        // Endpoint is required
        if (endpoint == null) {
            throw new CompilationException(PARAMETERS_REQUIRED, ENDPOINT_FIELD_NAME);
        }
        try {
            builder.endpoint(endpoint);
        } catch (Exception ex) {
            throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, ex, getMessageOrToString(ex));
        }

        // Shared Key
        if (accountName != null || accountKey != null) {
            if (accountName == null) {
                throw new CompilationException(REQUIRED_PARAM_IF_PARAM_IS_PRESENT, ACCOUNT_NAME_FIELD_NAME,
                        ACCOUNT_KEY_FIELD_NAME);
            }

            if (accountKey == null) {
                throw new CompilationException(REQUIRED_PARAM_IF_PARAM_IS_PRESENT, ACCOUNT_KEY_FIELD_NAME,
                        ACCOUNT_NAME_FIELD_NAME);
            }

            Optional<String> provided = getFirstNotNull(configuration, SHARED_ACCESS_SIGNATURE_FIELD_NAME,
                    MANAGED_IDENTITY_FIELD_NAME, CLIENT_ID_FIELD_NAME, CLIENT_SECRET_FIELD_NAME, TENANT_ID_FIELD_NAME);
            if (provided.isPresent()) {
                throw new CompilationException(PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT, provided.get(),
                        ACCOUNT_KEY_FIELD_NAME);
            }
            StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName, accountKey);
            builder.credential(credential);
        }

        // Shared access signature
        if (sharedAccessSignature != null) {
            Optional<String> provided = getFirstNotNull(configuration, MANAGED_IDENTITY_FIELD_NAME,
                    CLIENT_ID_FIELD_NAME, CLIENT_SECRET_FIELD_NAME, TENANT_ID_FIELD_NAME);
            if (provided.isPresent()) {
                throw new CompilationException(PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT, provided.get(),
                        SHARED_ACCESS_SIGNATURE_FIELD_NAME);
            }
            AzureSasCredential credential = new AzureSasCredential(sharedAccessSignature);
            builder.credential(credential);
        }

        // Managed Identity auth
        if (managedIdentity != null) {
            Optional<String> provided = getFirstNotNull(configuration, CLIENT_SECRET_FIELD_NAME, TENANT_ID_FIELD_NAME);
            if (provided.isPresent()) {
                throw new CompilationException(PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT, provided.get(),
                        MANAGED_IDENTITY_FIELD_NAME);
            }
            ManagedIdentityCredentialBuilder managedIdentityCredentialBuilder = new ManagedIdentityCredentialBuilder();
            if (clientId != null) {
                managedIdentityCredentialBuilder.clientId(clientId);
            }
            builder.credential(managedIdentityCredentialBuilder.build());
        }

        // Client secret
        if (clientSecret != null) {
            if (clientId == null) {
                throw new CompilationException(REQUIRED_PARAM_IF_PARAM_IS_PRESENT, CLIENT_ID_FIELD_NAME,
                        CLIENT_SECRET_FIELD_NAME);
            }

            // Tenant ID is required
            if (tenantId == null) {
                throw new CompilationException(REQUIRED_PARAM_IF_PARAM_IS_PRESENT, TENANT_ID_FIELD_NAME,
                        CLIENT_ID_FIELD_NAME);
            }

            ClientSecretCredentialBuilder secret = new ClientSecretCredentialBuilder();
            secret.clientId(clientId);
            secret.tenantId(tenantId);
            secret.clientSecret(clientSecret);
            builder.credential(secret.build());
        }

        // If client id is not present, ensure client secret, certificate, tenant id and client certificate
        // password are not present
        if (clientId == null) {
            Optional<String> provided = getFirstNotNull(configuration, CLIENT_SECRET_FIELD_NAME, TENANT_ID_FIELD_NAME);
            if (provided.isPresent()) {
                throw new CompilationException(PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT, provided.get(),
                        SHARED_ACCESS_SIGNATURE_FIELD_NAME);
            }
        }

        try {
            if (type == DataLakeServiceClient.class) {
                return type.cast(builder.buildClient());
            } else {
                return type.cast(builder.buildAsyncClient());
            }
        } catch (Exception ex) {
            throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, ex, getMessageOrToString(ex));
        }
    }

    public static List<PathItem> listDatalakePathItems(IApplicationContext appCtx, Map<String, String> configuration,
            AbstractExternalInputStreamFactory.IncludeExcludeMatcher includeExcludeMatcher,
            IWarningCollector warningCollector, ExternalDataPrefix externalDataPrefix,
            IExternalFilterEvaluator evaluator) throws CompilationException {
        DataLakeServiceClient client = buildClient(appCtx, configuration);
        return listDatalakePathItems(client, configuration, includeExcludeMatcher, warningCollector, externalDataPrefix,
                evaluator);
    }

    public static List<PathItem> listDatalakePathItems(DataLakeServiceClient client, Map<String, String> configuration,
            AbstractExternalInputStreamFactory.IncludeExcludeMatcher includeExcludeMatcher,
            IWarningCollector warningCollector, ExternalDataPrefix externalDataPrefix,
            IExternalFilterEvaluator evaluator) throws CompilationException {
        String container = configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME);
        List<PathItem> filesOnly = new ArrayList<>();

        try {
            DataLakeFileSystemClient fileSystemClient = client.getFileSystemClient(container);

            // Get all objects in a container and extract the paths to files
            ListPathsOptions listOptions = new ListPathsOptions();
            boolean recursive = configuration.containsKey(RECURSIVE_FIELD_NAME)
                    ? Boolean.parseBoolean(configuration.get(RECURSIVE_FIELD_NAME)) : DEFAULT_RECUSRIVE_VALUE;
            listOptions.setRecursive(recursive);
            listOptions.setPath(getPrefix(configuration, false, false));
            PagedIterable<PathItem> pathItems = fileSystemClient.listPaths(listOptions, null);

            // Collect the paths to files only
            collectAndFilterDatalakeFiles(pathItems, includeExcludeMatcher.getPredicate(),
                    includeExcludeMatcher.getMatchersList(), filesOnly, externalDataPrefix, evaluator,
                    warningCollector);

            // Warn if no files are returned
            if (filesOnly.isEmpty() && warningCollector.shouldWarn()) {
                Warning warning = Warning.of(null, ErrorCode.EXTERNAL_SOURCE_CONFIGURATION_RETURNED_NO_FILES);
                warningCollector.warn(warning);
            }
        } catch (Exception ex) {
            throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, ex, getMessageOrToString(ex));
        }

        return filesOnly;
    }

    /**
     * Collects and filters the files only, and excludes any folders
     *
     * @param items     storage items
     * @param predicate predicate to test with for file filtration
     * @param matchers  include/exclude matchers to test against
     * @param filesOnly List containing the files only (excluding folders)
     */
    private static void collectAndFilterDatalakeFiles(Iterable<PathItem> items,
            BiPredicate<List<Matcher>, String> predicate, List<Matcher> matchers, List<PathItem> filesOnly,
            ExternalDataPrefix externalDataPrefix, IExternalFilterEvaluator evaluator,
            IWarningCollector warningCollector) throws HyracksDataException {
        for (PathItem item : items) {
            String uri = item.getName();
            if (ExternalDataUtils.evaluate(uri, predicate, matchers, externalDataPrefix, evaluator, warningCollector)) {
                filesOnly.add(item);
            }
        }
    }

    /**
     * Validate external dataset properties
     *
     * @param configuration properties
     * @throws CompilationException Compilation exception
     */
    public static void validateAzureDataLakeProperties(Map<String, String> configuration, SourceLocation srcLoc,
            IWarningCollector collector, IApplicationContext appCtx) throws CompilationException {

        // check if the format property is present
        if (isDeltaTable(configuration)) {
            validateDeltaTableProperties(configuration);
        } else if (configuration.get(ExternalDataConstants.KEY_FORMAT) == null) {
            throw new CompilationException(ErrorCode.PARAMETERS_REQUIRED, srcLoc, ExternalDataConstants.KEY_FORMAT);
        }

        validateIncludeExclude(configuration);

        // Check if the bucket is present
        DataLakeServiceClient dataLakeServiceClient;
        try {
            String container = configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME);
            dataLakeServiceClient = buildClient(appCtx, configuration);
            DataLakeFileSystemClient fileSystemClient = dataLakeServiceClient.getFileSystemClient(container);

            // Get all objects in a container and extract the paths to files
            ListPathsOptions listPathsOptions = new ListPathsOptions();
            listPathsOptions.setPath(getPrefix(configuration));
            Iterable<PathItem> blobItems = fileSystemClient.listPaths(listPathsOptions, null);

            if (!blobItems.iterator().hasNext() && collector.shouldWarn()) {
                Warning warning = Warning.of(srcLoc, ErrorCode.EXTERNAL_SOURCE_CONFIGURATION_RETURNED_NO_FILES);
                collector.warn(warning);
            }
        } catch (CompilationException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, ex, getMessageOrToString(ex));
        }
    }

    public static String getEndpointFromClient(Map<String, String> configuration) throws CompilationException {
        String endpoint = configuration.get(ENDPOINT_FIELD_NAME);
        if (endpoint == null) {
            throw new CompilationException(PARAMETERS_REQUIRED, ENDPOINT_FIELD_NAME);
        }

        DataLakeServiceClientBuilder builder = new DataLakeServiceClientBuilder();
        try {
            builder.endpoint(endpoint);
        } catch (Exception ex) {
            throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, ex, getMessageOrToString(ex));
        }
        return AzureUtils.extractEndPoint(builder.buildClient().getAccountUrl());
    }

}
