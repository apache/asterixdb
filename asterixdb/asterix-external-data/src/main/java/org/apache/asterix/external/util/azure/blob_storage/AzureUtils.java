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
package org.apache.asterix.external.util.azure.blob_storage;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.asterix.common.exceptions.ErrorCode.EXTERNAL_SOURCE_ERROR;
import static org.apache.asterix.common.exceptions.ErrorCode.PARAMETERS_NOT_ALLOWED_AT_SAME_TIME;
import static org.apache.asterix.common.exceptions.ErrorCode.PARAMETERS_REQUIRED;
import static org.apache.asterix.common.exceptions.ErrorCode.PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT;
import static org.apache.asterix.common.exceptions.ErrorCode.REQUIRED_PARAM_IF_PARAM_IS_PRESENT;
import static org.apache.asterix.common.exceptions.ErrorCode.REQUIRED_PARAM_OR_PARAM_IF_PARAM_IS_PRESENT;
import static org.apache.asterix.external.util.ExternalDataUtils.getFirstNotNull;
import static org.apache.asterix.external.util.ExternalDataUtils.getPrefix;
import static org.apache.asterix.external.util.ExternalDataUtils.validateIncludeExclude;
import static org.apache.asterix.external.util.azure.blob_storage.AzureConstants.ACCOUNT_KEY_FIELD_NAME;
import static org.apache.asterix.external.util.azure.blob_storage.AzureConstants.ACCOUNT_NAME_FIELD_NAME;
import static org.apache.asterix.external.util.azure.blob_storage.AzureConstants.CLIENT_CERTIFICATE_FIELD_NAME;
import static org.apache.asterix.external.util.azure.blob_storage.AzureConstants.CLIENT_CERTIFICATE_PASSWORD_FIELD_NAME;
import static org.apache.asterix.external.util.azure.blob_storage.AzureConstants.CLIENT_ID_FIELD_NAME;
import static org.apache.asterix.external.util.azure.blob_storage.AzureConstants.CLIENT_SECRET_FIELD_NAME;
import static org.apache.asterix.external.util.azure.blob_storage.AzureConstants.ENDPOINT_FIELD_NAME;
import static org.apache.asterix.external.util.azure.blob_storage.AzureConstants.HADOOP_AZURE_BLOB_PROTOCOL;
import static org.apache.asterix.external.util.azure.blob_storage.AzureConstants.HADOOP_AZURE_FS_ACCOUNT_KEY;
import static org.apache.asterix.external.util.azure.blob_storage.AzureConstants.HADOOP_AZURE_FS_SAS;
import static org.apache.asterix.external.util.azure.blob_storage.AzureConstants.MANAGED_IDENTITY_ID_FIELD_NAME;
import static org.apache.asterix.external.util.azure.blob_storage.AzureConstants.RECURSIVE_FIELD_NAME;
import static org.apache.asterix.external.util.azure.blob_storage.AzureConstants.SHARED_ACCESS_SIGNATURE_FIELD_NAME;
import static org.apache.asterix.external.util.azure.blob_storage.AzureConstants.TENANT_ID_FIELD_NAME;
import static org.apache.hyracks.api.util.ExceptionUtils.getMessageOrToString;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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
import org.apache.asterix.external.util.HDFSUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.exceptions.Warning;

import com.azure.core.credential.AzureSasCredential;
import com.azure.core.http.rest.PagedIterable;
import com.azure.identity.ClientCertificateCredentialBuilder;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.models.ListPathsOptions;
import com.azure.storage.file.datalake.models.PathItem;

public class AzureUtils {
    private AzureUtils() {
        throw new AssertionError("do not instantiate");
    }

    /**
     * Builds the Azure storage account using the provided configuration
     *
     * @param configuration properties
     * @return client
     */
    public static BlobServiceClient buildAzureBlobClient(IApplicationContext appCtx, Map<String, String> configuration)
            throws CompilationException {
        String managedIdentityId = configuration.get(MANAGED_IDENTITY_ID_FIELD_NAME);
        String accountName = configuration.get(ACCOUNT_NAME_FIELD_NAME);
        String accountKey = configuration.get(ACCOUNT_KEY_FIELD_NAME);
        String sharedAccessSignature = configuration.get(SHARED_ACCESS_SIGNATURE_FIELD_NAME);
        String tenantId = configuration.get(TENANT_ID_FIELD_NAME);
        String clientId = configuration.get(CLIENT_ID_FIELD_NAME);
        String clientSecret = configuration.get(CLIENT_SECRET_FIELD_NAME);
        String clientCertificate = configuration.get(CLIENT_CERTIFICATE_FIELD_NAME);
        String clientCertificatePassword = configuration.get(CLIENT_CERTIFICATE_PASSWORD_FIELD_NAME);
        String endpoint = configuration.get(ENDPOINT_FIELD_NAME);

        // Client builder
        BlobServiceClientBuilder builder = new BlobServiceClientBuilder();
        int timeout = appCtx.getExternalProperties().getAzureRequestTimeout();
        RequestRetryOptions requestRetryOptions = new RequestRetryOptions(null, null, timeout, null, null, null);
        builder.retryOptions(requestRetryOptions);

        // Endpoint is required
        if (endpoint == null) {
            throw new CompilationException(PARAMETERS_REQUIRED, ENDPOINT_FIELD_NAME);
        }
        builder.endpoint(endpoint);

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
                    MANAGED_IDENTITY_ID_FIELD_NAME, CLIENT_ID_FIELD_NAME, CLIENT_SECRET_FIELD_NAME,
                    CLIENT_CERTIFICATE_FIELD_NAME, CLIENT_CERTIFICATE_PASSWORD_FIELD_NAME, TENANT_ID_FIELD_NAME);
            if (provided.isPresent()) {
                throw new CompilationException(PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT, provided.get(),
                        ACCOUNT_KEY_FIELD_NAME);
            }
            StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName, accountKey);
            builder.credential(credential);
        }

        // Shared access signature
        if (sharedAccessSignature != null) {
            Optional<String> provided = getFirstNotNull(configuration, MANAGED_IDENTITY_ID_FIELD_NAME,
                    CLIENT_ID_FIELD_NAME, CLIENT_SECRET_FIELD_NAME, CLIENT_CERTIFICATE_FIELD_NAME,
                    CLIENT_CERTIFICATE_PASSWORD_FIELD_NAME, TENANT_ID_FIELD_NAME);
            if (provided.isPresent()) {
                throw new CompilationException(PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT, provided.get(),
                        SHARED_ACCESS_SIGNATURE_FIELD_NAME);
            }
            AzureSasCredential credential = new AzureSasCredential(sharedAccessSignature);
            builder.credential(credential);
        }

        // Managed Identity auth
        if (managedIdentityId != null) {
            Optional<String> provided = getFirstNotNull(configuration, CLIENT_ID_FIELD_NAME, CLIENT_SECRET_FIELD_NAME,
                    CLIENT_CERTIFICATE_FIELD_NAME, CLIENT_CERTIFICATE_PASSWORD_FIELD_NAME, TENANT_ID_FIELD_NAME);
            if (provided.isPresent()) {
                throw new CompilationException(PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT, provided.get(),
                        MANAGED_IDENTITY_ID_FIELD_NAME);
            }
            builder.credential(new ManagedIdentityCredentialBuilder().clientId(managedIdentityId).build());
        }

        // Client secret & certificate auth
        if (clientId != null) {
            // Both (or neither) client secret and client secret were provided, only one is allowed
            if ((clientSecret == null) == (clientCertificate == null)) {
                if (clientSecret != null) {
                    throw new CompilationException(PARAMETERS_NOT_ALLOWED_AT_SAME_TIME, CLIENT_SECRET_FIELD_NAME,
                            CLIENT_CERTIFICATE_FIELD_NAME);
                } else {
                    throw new CompilationException(REQUIRED_PARAM_OR_PARAM_IF_PARAM_IS_PRESENT,
                            CLIENT_SECRET_FIELD_NAME, CLIENT_CERTIFICATE_FIELD_NAME, CLIENT_ID_FIELD_NAME);
                }
            }

            // Tenant ID is required
            if (tenantId == null) {
                throw new CompilationException(REQUIRED_PARAM_IF_PARAM_IS_PRESENT, TENANT_ID_FIELD_NAME,
                        CLIENT_ID_FIELD_NAME);
            }

            // Client certificate password is not allowed if client secret is used
            if (clientCertificatePassword != null && clientSecret != null) {
                throw new CompilationException(PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT,
                        CLIENT_CERTIFICATE_PASSWORD_FIELD_NAME, CLIENT_SECRET_FIELD_NAME);
            }

            // Use AD authentication
            if (clientSecret != null) {
                ClientSecretCredentialBuilder secret = new ClientSecretCredentialBuilder();
                secret.clientId(clientId);
                secret.tenantId(tenantId);
                secret.clientSecret(clientSecret);
                builder.credential(secret.build());
            } else {
                // Certificate
                ClientCertificateCredentialBuilder certificate = new ClientCertificateCredentialBuilder();
                certificate.clientId(clientId);
                certificate.tenantId(tenantId);
                try {
                    InputStream certificateContent = new ByteArrayInputStream(clientCertificate.getBytes(UTF_8));
                    if (clientCertificatePassword == null) {
                        Method pemCertificate = ClientCertificateCredentialBuilder.class
                                .getDeclaredMethod("pemCertificate", InputStream.class);
                        pemCertificate.setAccessible(true);
                        pemCertificate.invoke(certificate, certificateContent);
                    } else {
                        Method pemCertificate = ClientCertificateCredentialBuilder.class
                                .getDeclaredMethod("pfxCertificate", InputStream.class, String.class);
                        pemCertificate.setAccessible(true);
                        pemCertificate.invoke(certificate, certificateContent, clientCertificatePassword);
                    }
                } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException ex) {
                    throw new CompilationException(EXTERNAL_SOURCE_ERROR, ex, ex.getMessage());
                }
                builder.credential(certificate.build());
            }
        }

        // If client id is not present, ensure client secret, certificate, tenant id and client certificate
        // password are not present
        if (clientId == null) {
            Optional<String> provided = getFirstNotNull(configuration, CLIENT_SECRET_FIELD_NAME,
                    CLIENT_CERTIFICATE_FIELD_NAME, CLIENT_CERTIFICATE_PASSWORD_FIELD_NAME, TENANT_ID_FIELD_NAME);
            if (provided.isPresent()) {
                throw new CompilationException(PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT, provided.get(),
                        SHARED_ACCESS_SIGNATURE_FIELD_NAME);
            }
        }

        try {
            return builder.buildClient();
        } catch (Exception ex) {
            throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, ex, getMessageOrToString(ex));
        }
    }

    /**
     * Builds the Azure data lake storage account using the provided configuration
     *
     * @param configuration properties
     * @return client
     */
    public static DataLakeServiceClient buildAzureDatalakeClient(IApplicationContext appCtx,
            Map<String, String> configuration) throws CompilationException {
        String managedIdentityId = configuration.get(MANAGED_IDENTITY_ID_FIELD_NAME);
        String accountName = configuration.get(ACCOUNT_NAME_FIELD_NAME);
        String accountKey = configuration.get(ACCOUNT_KEY_FIELD_NAME);
        String sharedAccessSignature = configuration.get(SHARED_ACCESS_SIGNATURE_FIELD_NAME);
        String tenantId = configuration.get(TENANT_ID_FIELD_NAME);
        String clientId = configuration.get(CLIENT_ID_FIELD_NAME);
        String clientSecret = configuration.get(CLIENT_SECRET_FIELD_NAME);
        String clientCertificate = configuration.get(CLIENT_CERTIFICATE_FIELD_NAME);
        String clientCertificatePassword = configuration.get(CLIENT_CERTIFICATE_PASSWORD_FIELD_NAME);
        String endpoint = configuration.get(ENDPOINT_FIELD_NAME);

        // Client builder
        DataLakeServiceClientBuilder builder = new DataLakeServiceClientBuilder();
        int timeout = appCtx.getExternalProperties().getAzureRequestTimeout();
        RequestRetryOptions requestRetryOptions = new RequestRetryOptions(null, null, timeout, null, null, null);
        builder.retryOptions(requestRetryOptions);

        // Endpoint is required
        if (endpoint == null) {
            throw new CompilationException(PARAMETERS_REQUIRED, ENDPOINT_FIELD_NAME);
        }
        builder.endpoint(endpoint);

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
                    MANAGED_IDENTITY_ID_FIELD_NAME, CLIENT_ID_FIELD_NAME, CLIENT_SECRET_FIELD_NAME,
                    CLIENT_CERTIFICATE_FIELD_NAME, CLIENT_CERTIFICATE_PASSWORD_FIELD_NAME, TENANT_ID_FIELD_NAME);
            if (provided.isPresent()) {
                throw new CompilationException(PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT, provided.get(),
                        ACCOUNT_KEY_FIELD_NAME);
            }
            StorageSharedKeyCredential credential = new StorageSharedKeyCredential(accountName, accountKey);
            builder.credential(credential);
        }

        // Shared access signature
        if (sharedAccessSignature != null) {
            Optional<String> provided = getFirstNotNull(configuration, MANAGED_IDENTITY_ID_FIELD_NAME,
                    CLIENT_ID_FIELD_NAME, CLIENT_SECRET_FIELD_NAME, CLIENT_CERTIFICATE_FIELD_NAME,
                    CLIENT_CERTIFICATE_PASSWORD_FIELD_NAME, TENANT_ID_FIELD_NAME);
            if (provided.isPresent()) {
                throw new CompilationException(PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT, provided.get(),
                        SHARED_ACCESS_SIGNATURE_FIELD_NAME);
            }
            AzureSasCredential credential = new AzureSasCredential(sharedAccessSignature);
            builder.credential(credential);
        }

        // Managed Identity auth
        if (managedIdentityId != null) {
            Optional<String> provided = getFirstNotNull(configuration, CLIENT_ID_FIELD_NAME, CLIENT_SECRET_FIELD_NAME,
                    CLIENT_CERTIFICATE_FIELD_NAME, CLIENT_CERTIFICATE_PASSWORD_FIELD_NAME, TENANT_ID_FIELD_NAME);
            if (provided.isPresent()) {
                throw new CompilationException(PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT, provided.get(),
                        MANAGED_IDENTITY_ID_FIELD_NAME);
            }
            builder.credential(new ManagedIdentityCredentialBuilder().clientId(managedIdentityId).build());
        }

        // Client secret & certificate auth
        if (clientId != null) {
            // Both (or neither) client secret and client secret were provided, only one is allowed
            if ((clientSecret == null) == (clientCertificate == null)) {
                if (clientSecret != null) {
                    throw new CompilationException(PARAMETERS_NOT_ALLOWED_AT_SAME_TIME, CLIENT_SECRET_FIELD_NAME,
                            CLIENT_CERTIFICATE_FIELD_NAME);
                } else {
                    throw new CompilationException(REQUIRED_PARAM_OR_PARAM_IF_PARAM_IS_PRESENT,
                            CLIENT_SECRET_FIELD_NAME, CLIENT_CERTIFICATE_FIELD_NAME, CLIENT_ID_FIELD_NAME);
                }
            }

            // Tenant ID is required
            if (tenantId == null) {
                throw new CompilationException(REQUIRED_PARAM_IF_PARAM_IS_PRESENT, TENANT_ID_FIELD_NAME,
                        CLIENT_ID_FIELD_NAME);
            }

            // Client certificate password is not allowed if client secret is used
            if (clientCertificatePassword != null && clientSecret != null) {
                throw new CompilationException(PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT,
                        CLIENT_CERTIFICATE_PASSWORD_FIELD_NAME, CLIENT_SECRET_FIELD_NAME);
            }

            // Use AD authentication
            if (clientSecret != null) {
                ClientSecretCredentialBuilder secret = new ClientSecretCredentialBuilder();
                secret.clientId(clientId);
                secret.tenantId(tenantId);
                secret.clientSecret(clientSecret);
                builder.credential(secret.build());
            } else {
                // Certificate
                ClientCertificateCredentialBuilder certificate = new ClientCertificateCredentialBuilder();
                certificate.clientId(clientId);
                certificate.tenantId(tenantId);
                try {
                    InputStream certificateContent = new ByteArrayInputStream(clientCertificate.getBytes(UTF_8));
                    if (clientCertificatePassword == null) {
                        Method pemCertificate = ClientCertificateCredentialBuilder.class
                                .getDeclaredMethod("pemCertificate", InputStream.class);
                        pemCertificate.setAccessible(true);
                        pemCertificate.invoke(certificate, certificateContent);
                    } else {
                        Method pemCertificate = ClientCertificateCredentialBuilder.class
                                .getDeclaredMethod("pfxCertificate", InputStream.class, String.class);
                        pemCertificate.setAccessible(true);
                        pemCertificate.invoke(certificate, certificateContent, clientCertificatePassword);
                    }
                } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException ex) {
                    throw new CompilationException(EXTERNAL_SOURCE_ERROR, ex, getMessageOrToString(ex));
                }
                builder.credential(certificate.build());
            }
        }

        // If client id is not present, ensure client secret, certificate, tenant id and client certificate
        // password are not present
        if (clientId == null) {
            Optional<String> provided = getFirstNotNull(configuration, CLIENT_SECRET_FIELD_NAME,
                    CLIENT_CERTIFICATE_FIELD_NAME, CLIENT_CERTIFICATE_PASSWORD_FIELD_NAME, TENANT_ID_FIELD_NAME);
            if (provided.isPresent()) {
                throw new CompilationException(PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT, provided.get(),
                        SHARED_ACCESS_SIGNATURE_FIELD_NAME);
            }
        }

        try {
            return builder.buildClient();
        } catch (Exception ex) {
            throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, ex, getMessageOrToString(ex));
        }
    }

    public static List<BlobItem> listBlobItems(IApplicationContext context, Map<String, String> configuration,
            AbstractExternalInputStreamFactory.IncludeExcludeMatcher includeExcludeMatcher,
            IWarningCollector warningCollector, ExternalDataPrefix externalDataPrefix,
            IExternalFilterEvaluator evaluator) throws CompilationException {
        BlobServiceClient blobServiceClient = buildAzureBlobClient(context, configuration);
        return listBlobItems(blobServiceClient, configuration, includeExcludeMatcher, warningCollector,
                externalDataPrefix, evaluator);
    }

    public static List<BlobItem> listBlobItems(BlobServiceClient blobServiceClient, Map<String, String> configuration,
            AbstractExternalInputStreamFactory.IncludeExcludeMatcher includeExcludeMatcher,
            IWarningCollector warningCollector, ExternalDataPrefix externalDataPrefix,
            IExternalFilterEvaluator evaluator) throws CompilationException {
        String container = configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME);
        List<BlobItem> filesOnly = new ArrayList<>();

        try {
            BlobContainerClient blobContainer = blobServiceClient.getBlobContainerClient(container);

            // Get all objects in a container and extract the paths to files
            ListBlobsOptions listBlobsOptions = new ListBlobsOptions();
            listBlobsOptions.setPrefix(getPrefix(configuration));
            Iterable<BlobItem> blobItems = blobContainer.listBlobs(listBlobsOptions, null);

            // Collect the paths to files only
            collectAndFilterBlobFiles(blobItems, includeExcludeMatcher.getPredicate(),
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
    private static void collectAndFilterBlobFiles(Iterable<BlobItem> items,
            BiPredicate<List<Matcher>, String> predicate, List<Matcher> matchers, List<BlobItem> filesOnly,
            ExternalDataPrefix externalDataPrefix, IExternalFilterEvaluator evaluator,
            IWarningCollector warningCollector) throws HyracksDataException {
        for (BlobItem item : items) {
            if (ExternalDataUtils.evaluate(item.getName(), predicate, matchers, externalDataPrefix, evaluator,
                    warningCollector)) {
                filesOnly.add(item);
            }
        }
    }

    public static List<PathItem> listDatalakePathItems(DataLakeServiceClient client, Map<String, String> configuration,
            AbstractExternalInputStreamFactory.IncludeExcludeMatcher includeExcludeMatcher,
            IWarningCollector warningCollector) throws CompilationException {
        String container = configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME);

        List<PathItem> filesOnly = new ArrayList<>();

        DataLakeFileSystemClient fileSystemClient;
        try {
            fileSystemClient = client.getFileSystemClient(container);

            // Get all objects in a container and extract the paths to files
            ListPathsOptions listOptions = new ListPathsOptions();
            boolean recursive = Boolean.parseBoolean(configuration.get(RECURSIVE_FIELD_NAME));
            listOptions.setRecursive(recursive);
            listOptions.setPath(getPrefix(configuration, false));
            PagedIterable<PathItem> pathItems = fileSystemClient.listPaths(listOptions, null);

            // Collect the paths to files only
            collectAndFilterDatalakeFiles(pathItems, includeExcludeMatcher.getPredicate(),
                    includeExcludeMatcher.getMatchersList(), filesOnly);

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
            BiPredicate<List<Matcher>, String> predicate, List<Matcher> matchers, List<PathItem> filesOnly) {
        for (PathItem item : items) {
            String uri = item.getName();

            // skip folders
            if (uri.endsWith("/")) {
                continue;
            }

            // No filter, add file
            if (predicate.test(matchers, uri)) {
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
    public static void validateAzureBlobProperties(Map<String, String> configuration, SourceLocation srcLoc,
            IWarningCollector collector, IApplicationContext appCtx) throws CompilationException {

        // check if the format property is present
        if (configuration.get(ExternalDataConstants.KEY_FORMAT) == null) {
            throw new CompilationException(ErrorCode.PARAMETERS_REQUIRED, srcLoc, ExternalDataConstants.KEY_FORMAT);
        }

        validateIncludeExclude(configuration);
        try {
            // TODO(htowaileb): maybe something better, this will check to ensure type is supported before creation
            new ExternalDataPrefix(configuration);
        } catch (AlgebricksException ex) {
            throw new CompilationException(ErrorCode.FAILED_TO_CALCULATE_COMPUTED_FIELDS, ex);
        }

        // Check if the bucket is present
        BlobServiceClient blobServiceClient;
        try {
            String container = configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME);
            blobServiceClient = buildAzureBlobClient(appCtx, configuration);
            BlobContainerClient blobContainer = blobServiceClient.getBlobContainerClient(container);

            // Get all objects in a container and extract the paths to files
            ListBlobsOptions listBlobsOptions = new ListBlobsOptions();
            listBlobsOptions.setPrefix(getPrefix(configuration));
            Iterable<BlobItem> blobItems = blobContainer.listBlobs(listBlobsOptions, null);

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

    /**
     * Validate external dataset properties
     *
     * @param configuration properties
     * @throws CompilationException Compilation exception
     */
    public static void validateAzureDataLakeProperties(Map<String, String> configuration, SourceLocation srcLoc,
            IWarningCollector collector, IApplicationContext appCtx) throws CompilationException {

        // check if the format property is present
        if (configuration.get(ExternalDataConstants.KEY_FORMAT) == null) {
            throw new CompilationException(ErrorCode.PARAMETERS_REQUIRED, srcLoc, ExternalDataConstants.KEY_FORMAT);
        }

        validateIncludeExclude(configuration);

        // Check if the bucket is present
        DataLakeServiceClient dataLakeServiceClient;
        try {
            String container = configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME);
            dataLakeServiceClient = buildAzureDatalakeClient(appCtx, configuration);
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

    /**
     * Builds the Azure Blob storage client using the provided configuration
     *
     * @param configuration properties
     * @see <a href="https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/azure/azure-storage">Azure
     * Blob storage</a>
     */
    public static void configureAzureHdfsJobConf(JobConf conf, Map<String, String> configuration, String endPoint) {
        String container = configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME);
        String accountKey = configuration.get(ACCOUNT_KEY_FIELD_NAME);
        String sharedAccessSignature = configuration.get(SHARED_ACCESS_SIGNATURE_FIELD_NAME);

        //Disable caching S3 FileSystem
        HDFSUtils.disableHadoopFileSystemCache(conf, HADOOP_AZURE_BLOB_PROTOCOL);

        //Key for Hadoop configuration
        StringBuilder hadoopKey = new StringBuilder();
        //Value for Hadoop configuration
        String hadoopValue;
        if (accountKey != null || sharedAccessSignature != null) {
            if (accountKey != null) {
                hadoopKey.append(HADOOP_AZURE_FS_ACCOUNT_KEY).append('.');
                //Set only the AccountKey
                hadoopValue = accountKey;
            } else {
                //Use SAS for Hadoop FS as connectionString is provided
                hadoopKey.append(HADOOP_AZURE_FS_SAS).append('.');
                //Setting the container is required for SAS
                hadoopKey.append(container).append('.');
                //Set the connection string for SAS
                hadoopValue = sharedAccessSignature;
            }
            //Set the endPoint, which includes the AccountName
            hadoopKey.append(endPoint);
            //Tells Hadoop we are reading from Blob Storage
            conf.set(hadoopKey.toString(), hadoopValue);
        }
    }
}
