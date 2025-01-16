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
package org.apache.asterix.external.util.aws.s3;

import static org.apache.asterix.common.exceptions.ErrorCode.INVALID_PARAM_VALUE_ALLOWED_VALUE;
import static org.apache.asterix.common.exceptions.ErrorCode.PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT;
import static org.apache.asterix.common.exceptions.ErrorCode.REQUIRED_PARAM_IF_PARAM_IS_PRESENT;
import static org.apache.asterix.common.exceptions.ErrorCode.S3_REGION_NOT_SUPPORTED;
import static org.apache.asterix.external.util.ExternalDataUtils.getPrefix;
import static org.apache.asterix.external.util.ExternalDataUtils.isDeltaTable;
import static org.apache.asterix.external.util.ExternalDataUtils.validateDeltaTableExists;
import static org.apache.asterix.external.util.ExternalDataUtils.validateDeltaTableProperties;
import static org.apache.asterix.external.util.ExternalDataUtils.validateIncludeExclude;
import static org.apache.asterix.external.util.aws.s3.S3Constants.ACCESS_KEY_ID_FIELD_NAME;
import static org.apache.asterix.external.util.aws.s3.S3Constants.CROSS_REGION_FIELD_NAME;
import static org.apache.asterix.external.util.aws.s3.S3Constants.ERROR_EXPIRED_TOKEN;
import static org.apache.asterix.external.util.aws.s3.S3Constants.ERROR_INTERNAL_ERROR;
import static org.apache.asterix.external.util.aws.s3.S3Constants.ERROR_METHOD_NOT_IMPLEMENTED;
import static org.apache.asterix.external.util.aws.s3.S3Constants.ERROR_SLOW_DOWN;
import static org.apache.asterix.external.util.aws.s3.S3Constants.EXTERNAL_ID_FIELD_NAME;
import static org.apache.asterix.external.util.aws.s3.S3Constants.HADOOP_ACCESS_KEY_ID;
import static org.apache.asterix.external.util.aws.s3.S3Constants.HADOOP_ANONYMOUS;
import static org.apache.asterix.external.util.aws.s3.S3Constants.HADOOP_ASSUMED_ROLE;
import static org.apache.asterix.external.util.aws.s3.S3Constants.HADOOP_ASSUME_ROLE_ARN;
import static org.apache.asterix.external.util.aws.s3.S3Constants.HADOOP_ASSUME_ROLE_EXTERNAL_ID;
import static org.apache.asterix.external.util.aws.s3.S3Constants.HADOOP_ASSUME_ROLE_SESSION_DURATION;
import static org.apache.asterix.external.util.aws.s3.S3Constants.HADOOP_ASSUME_ROLE_SESSION_NAME;
import static org.apache.asterix.external.util.aws.s3.S3Constants.HADOOP_CREDENTIALS_TO_ASSUME_ROLE_KEY;
import static org.apache.asterix.external.util.aws.s3.S3Constants.HADOOP_CREDENTIAL_PROVIDER_KEY;
import static org.apache.asterix.external.util.aws.s3.S3Constants.HADOOP_INSTANCE_PROFILE;
import static org.apache.asterix.external.util.aws.s3.S3Constants.HADOOP_PATH_STYLE_ACCESS;
import static org.apache.asterix.external.util.aws.s3.S3Constants.HADOOP_REGION;
import static org.apache.asterix.external.util.aws.s3.S3Constants.HADOOP_S3_CONNECTION_POOL_SIZE;
import static org.apache.asterix.external.util.aws.s3.S3Constants.HADOOP_SECRET_ACCESS_KEY;
import static org.apache.asterix.external.util.aws.s3.S3Constants.HADOOP_SERVICE_END_POINT;
import static org.apache.asterix.external.util.aws.s3.S3Constants.HADOOP_SESSION_TOKEN;
import static org.apache.asterix.external.util.aws.s3.S3Constants.HADOOP_SIMPLE;
import static org.apache.asterix.external.util.aws.s3.S3Constants.INSTANCE_PROFILE_FIELD_NAME;
import static org.apache.asterix.external.util.aws.s3.S3Constants.REGION_FIELD_NAME;
import static org.apache.asterix.external.util.aws.s3.S3Constants.ROLE_ARN_FIELD_NAME;
import static org.apache.asterix.external.util.aws.s3.S3Constants.SECRET_ACCESS_KEY_FIELD_NAME;
import static org.apache.asterix.external.util.aws.s3.S3Constants.SERVICE_END_POINT_FIELD_NAME;
import static org.apache.asterix.external.util.aws.s3.S3Constants.SESSION_TOKEN_FIELD_NAME;
import static org.apache.hyracks.api.util.ExceptionUtils.getMessageOrToString;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.external.IExternalCredentialsCache;
import org.apache.asterix.common.external.IExternalCredentialsCacheUpdater;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataPrefix;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.api.util.CleanupUtils;

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Response;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

public class S3AuthUtils {
    enum AuthenticationType {
        ANONYMOUS,
        ARN,
        INSTANCE_PROFILE,
        ACCESS_KEYS,
        BAD_AUTHENTICATION
    }

    private S3AuthUtils() {
        throw new AssertionError("do not instantiate");
    }

    public static boolean isRetryableError(String errorCode) {
        return errorCode.equals(ERROR_INTERNAL_ERROR) || errorCode.equals(ERROR_SLOW_DOWN);
    }

    public static boolean isArnAssumedRoleExpiredToken(Map<String, String> configuration, String errorCode) {
        return ERROR_EXPIRED_TOKEN.equals(errorCode) && getAuthenticationType(configuration) == AuthenticationType.ARN;
    }

    /**
     * Builds the S3 client using the provided configuration
     *
     * @param configuration properties
     * @return S3 client
     * @throws CompilationException CompilationException
     */
    public static S3Client buildAwsS3Client(IApplicationContext appCtx, Map<String, String> configuration)
            throws CompilationException {
        String regionId = configuration.get(REGION_FIELD_NAME);
        String serviceEndpoint = configuration.get(SERVICE_END_POINT_FIELD_NAME);

        Region region = validateAndGetRegion(regionId);
        boolean crossRegion = validateAndGetCrossRegion(configuration.get(CROSS_REGION_FIELD_NAME));
        AwsCredentialsProvider credentialsProvider = buildCredentialsProvider(appCtx, configuration);

        S3ClientBuilder builder = S3Client.builder();
        builder.region(region);
        builder.crossRegionAccessEnabled(crossRegion);
        builder.credentialsProvider(credentialsProvider);

        // Validate the service endpoint if present
        if (serviceEndpoint != null) {
            try {
                URI uri = new URI(serviceEndpoint);
                try {
                    builder.endpointOverride(uri);
                } catch (NullPointerException ex) {
                    throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, ex, getMessageOrToString(ex));
                }
            } catch (URISyntaxException ex) {
                throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, ex,
                        String.format("Invalid service endpoint %s", serviceEndpoint));
            }
        }

        return builder.build();
    }

    public static AwsCredentialsProvider buildCredentialsProvider(IApplicationContext appCtx,
            Map<String, String> configuration) throws CompilationException {
        AuthenticationType authenticationType = getAuthenticationType(configuration);
        switch (authenticationType) {
            case ANONYMOUS:
                return AnonymousCredentialsProvider.create();
            case ARN:
                return getTrustAccountCredentials(appCtx, configuration);
            case INSTANCE_PROFILE:
                return getInstanceProfileCredentials(configuration);
            case ACCESS_KEYS:
                return getAccessKeyCredentials(configuration);
            default:
                // missing required creds, report correct error message
                String externalId = configuration.get(EXTERNAL_ID_FIELD_NAME);
                if (externalId != null) {
                    throw new CompilationException(REQUIRED_PARAM_IF_PARAM_IS_PRESENT, ROLE_ARN_FIELD_NAME,
                            EXTERNAL_ID_FIELD_NAME);
                } else {
                    throw new CompilationException(REQUIRED_PARAM_IF_PARAM_IS_PRESENT, ACCESS_KEY_ID_FIELD_NAME,
                            SESSION_TOKEN_FIELD_NAME);
                }
        }
    }

    public static Region validateAndGetRegion(String regionId) throws CompilationException {
        List<Region> regions = S3Client.serviceMetadata().regions();
        Optional<Region> selectedRegion = regions.stream().filter(region -> region.id().equals(regionId)).findFirst();

        if (selectedRegion.isEmpty()) {
            throw new CompilationException(S3_REGION_NOT_SUPPORTED, regionId);
        }
        return selectedRegion.get();
    }

    private static AuthenticationType getAuthenticationType(Map<String, String> configuration) {
        String roleArn = configuration.get(ROLE_ARN_FIELD_NAME);
        String instanceProfile = configuration.get(INSTANCE_PROFILE_FIELD_NAME);
        String accessKeyId = configuration.get(ACCESS_KEY_ID_FIELD_NAME);
        String secretAccessKey = configuration.get(SECRET_ACCESS_KEY_FIELD_NAME);

        if (noAuth(configuration)) {
            return AuthenticationType.ANONYMOUS;
        } else if (roleArn != null) {
            return AuthenticationType.ARN;
        } else if (instanceProfile != null) {
            return AuthenticationType.INSTANCE_PROFILE;
        } else if (accessKeyId != null || secretAccessKey != null) {
            return AuthenticationType.ACCESS_KEYS;
        } else {
            return AuthenticationType.BAD_AUTHENTICATION;
        }
    }

    public static boolean validateAndGetCrossRegion(String crossRegion) throws CompilationException {
        if (crossRegion == null) {
            return false;
        }
        if (!"true".equalsIgnoreCase(crossRegion) && !"false".equalsIgnoreCase(crossRegion)) {
            throw new CompilationException(INVALID_PARAM_VALUE_ALLOWED_VALUE, CROSS_REGION_FIELD_NAME, "true, false");
        }
        return Boolean.parseBoolean(crossRegion);
    }

    private static boolean noAuth(Map<String, String> configuration) {
        return getNonNull(configuration, INSTANCE_PROFILE_FIELD_NAME, ROLE_ARN_FIELD_NAME, EXTERNAL_ID_FIELD_NAME,
                ACCESS_KEY_ID_FIELD_NAME, SECRET_ACCESS_KEY_FIELD_NAME, SESSION_TOKEN_FIELD_NAME) == null;
    }

    /**
     * Returns the cached credentials if valid, otherwise, generates new credentials by assume a role
     *
     * @param appCtx application context
     * @param configuration configuration
     * @return returns the cached credentials if valid, otherwise, generates new credentials by assume a role
     * @throws CompilationException CompilationException
     */
    public static AwsCredentialsProvider getTrustAccountCredentials(IApplicationContext appCtx,
            Map<String, String> configuration) throws CompilationException {
        IExternalCredentialsCache cache = appCtx.getExternalCredentialsCache();
        Object credentialsObject = cache.getCredentials(configuration);
        if (credentialsObject != null) {
            return () -> (AwsSessionCredentials) credentialsObject;
        }
        IExternalCredentialsCacheUpdater cacheUpdater = appCtx.getExternalCredentialsCacheUpdater();
        AwsSessionCredentials credentials;
        try {
            credentials = (AwsSessionCredentials) cacheUpdater.generateAndCacheCredentials(configuration);
        } catch (HyracksDataException ex) {
            throw new CompilationException(ErrorCode.FAILED_EXTERNAL_CROSS_ACCOUNT_AUTHENTICATION, ex, ex.getMessage());
        }

        return () -> credentials;
    }

    /**
     * Assume role using provided credentials and return the new credentials
     *
     * @param configuration configuration
     * @return return credentials from the assume role
     * @throws CompilationException CompilationException
     */
    public static AwsCredentialsProvider assumeRoleAndGetCredentials(Map<String, String> configuration)
            throws CompilationException {
        String regionId = configuration.get(REGION_FIELD_NAME);
        String arnRole = configuration.get(ROLE_ARN_FIELD_NAME);
        String externalId = configuration.get(EXTERNAL_ID_FIELD_NAME);
        Region region = validateAndGetRegion(regionId);

        AssumeRoleRequest.Builder builder = AssumeRoleRequest.builder();
        builder.roleArn(arnRole);
        builder.roleSessionName(UUID.randomUUID().toString());
        builder.durationSeconds(900); // TODO(htowaileb): configurable? Can be 900 to 43200 (15 mins to 12 hours)
        if (externalId != null) {
            builder.externalId(externalId);
        }

        // credentials to be used to assume the role
        AwsCredentialsProvider credentialsProvider;
        AssumeRoleRequest request = builder.build();
        String instanceProfile = configuration.get(INSTANCE_PROFILE_FIELD_NAME);
        String accessKeyId = configuration.get(ACCESS_KEY_ID_FIELD_NAME);
        String secretAccessKey = configuration.get(SECRET_ACCESS_KEY_FIELD_NAME);
        if ("true".equalsIgnoreCase(instanceProfile)) {
            credentialsProvider = getInstanceProfileCredentials(configuration, true);
        } else if (accessKeyId != null && secretAccessKey != null) {
            credentialsProvider = getAccessKeyCredentials(configuration, true);
        } else {
            throw new CompilationException(ErrorCode.NO_AWS_VALID_PARAMS_FOUND_FOR_CROSS_ACCOUNT_TRUST_AUTHENTICATION);
        }

        // assume the role from the provided arn
        try (StsClient stsClient =
                StsClient.builder().region(region).credentialsProvider(credentialsProvider).build()) {
            AssumeRoleResponse response = stsClient.assumeRole(request);
            Credentials credentials = response.credentials();
            return StaticCredentialsProvider.create(AwsSessionCredentials.create(credentials.accessKeyId(),
                    credentials.secretAccessKey(), credentials.sessionToken()));
        } catch (SdkException ex) {
            throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, ex, getMessageOrToString(ex));
        }
    }

    private static AwsCredentialsProvider getInstanceProfileCredentials(Map<String, String> configuration)
            throws CompilationException {
        return getInstanceProfileCredentials(configuration, false);
    }

    private static AwsCredentialsProvider getInstanceProfileCredentials(Map<String, String> configuration,
            boolean assumeRoleAuthentication) throws CompilationException {
        String instanceProfile = configuration.get(INSTANCE_PROFILE_FIELD_NAME);

        // only "true" value is allowed
        if (!"true".equalsIgnoreCase(instanceProfile)) {
            throw new CompilationException(INVALID_PARAM_VALUE_ALLOWED_VALUE, INSTANCE_PROFILE_FIELD_NAME, "true");
        }

        if (!assumeRoleAuthentication) {
            String notAllowed = getNonNull(configuration, ACCESS_KEY_ID_FIELD_NAME, SECRET_ACCESS_KEY_FIELD_NAME,
                    SESSION_TOKEN_FIELD_NAME);
            if (notAllowed != null) {
                throw new CompilationException(PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT, notAllowed,
                        INSTANCE_PROFILE_FIELD_NAME);
            }
        }
        return InstanceProfileCredentialsProvider.create();
    }

    private static AwsCredentialsProvider getAccessKeyCredentials(Map<String, String> configuration)
            throws CompilationException {
        return getAccessKeyCredentials(configuration, false);
    }

    private static AwsCredentialsProvider getAccessKeyCredentials(Map<String, String> configuration,
            boolean assumeRoleAuthentication) throws CompilationException {
        String accessKeyId = configuration.get(ACCESS_KEY_ID_FIELD_NAME);
        String secretAccessKey = configuration.get(SECRET_ACCESS_KEY_FIELD_NAME);
        String sessionToken = configuration.get(SESSION_TOKEN_FIELD_NAME);

        if (accessKeyId == null) {
            throw new CompilationException(REQUIRED_PARAM_IF_PARAM_IS_PRESENT, ACCESS_KEY_ID_FIELD_NAME,
                    SECRET_ACCESS_KEY_FIELD_NAME);
        }
        if (secretAccessKey == null) {
            throw new CompilationException(REQUIRED_PARAM_IF_PARAM_IS_PRESENT, SECRET_ACCESS_KEY_FIELD_NAME,
                    ACCESS_KEY_ID_FIELD_NAME);
        }

        if (!assumeRoleAuthentication) {
            String notAllowed = getNonNull(configuration, INSTANCE_PROFILE_FIELD_NAME, EXTERNAL_ID_FIELD_NAME);
            if (notAllowed != null) {
                throw new CompilationException(PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT, notAllowed,
                        INSTANCE_PROFILE_FIELD_NAME);
            }
        }

        // use session token if provided
        if (sessionToken != null) {
            return StaticCredentialsProvider
                    .create(AwsSessionCredentials.create(accessKeyId, secretAccessKey, sessionToken));
        } else {
            return StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey));
        }
    }

    private static String getNonNull(Map<String, String> configuration, String... fieldNames) {
        for (String fieldName : fieldNames) {
            if (configuration.get(fieldName) != null) {
                return fieldName;
            }
        }
        return null;
    }

    public static void configureAwsS3HdfsJobConf(IApplicationContext appCtx, JobConf conf,
            Map<String, String> configuration) throws CompilationException {
        configureAwsS3HdfsJobConf(appCtx, conf, configuration, 0);
    }

    /**
     * Builds the S3 client using the provided configuration
     *
     * @param appCtx application context
     * @param configuration      properties
     * @param numberOfPartitions number of partitions in the cluster
     */
    public static void configureAwsS3HdfsJobConf(IApplicationContext appCtx, JobConf jobConf,
            Map<String, String> configuration, int numberOfPartitions) throws CompilationException {
        setHadoopCredentials(jobConf, configuration);
        String serviceEndpoint = configuration.get(SERVICE_END_POINT_FIELD_NAME);
        Region region = validateAndGetRegion(configuration.get(REGION_FIELD_NAME));
        jobConf.set(HADOOP_REGION, region.toString());
        if (serviceEndpoint != null) {
            // Validation of the URL should be done at hadoop-aws level
            jobConf.set(HADOOP_SERVICE_END_POINT, serviceEndpoint);
        } else {
            //Region is ignored and buckets could be found by the central endpoint
            jobConf.set(HADOOP_SERVICE_END_POINT, Constants.CENTRAL_ENDPOINT);
        }

        /*
         * This is to allow S3 definition to have path-style form. Should always be true to match the current
         * way we access files in S3
         */
        jobConf.set(HADOOP_PATH_STYLE_ACCESS, ExternalDataConstants.TRUE);

        /*
         * Set the size of S3 connection pool to be the number of partitions
         */
        if (numberOfPartitions != 0) {
            jobConf.set(HADOOP_S3_CONNECTION_POOL_SIZE, String.valueOf(numberOfPartitions));
        }
    }

    /**
     * Sets the credentials provider type and the credentials to hadoop based on the provided configuration
     *
     * @param jobConf hadoop job config
     * @param configuration external details configuration
     */
    private static void setHadoopCredentials(JobConf jobConf, Map<String, String> configuration) {
        AuthenticationType authenticationType = getAuthenticationType(configuration);
        switch (authenticationType) {
            case ANONYMOUS:
                jobConf.set(HADOOP_CREDENTIAL_PROVIDER_KEY, HADOOP_ANONYMOUS);
                break;
            case ARN:
                jobConf.set(HADOOP_CREDENTIAL_PROVIDER_KEY, HADOOP_ASSUMED_ROLE);
                jobConf.set(HADOOP_ASSUME_ROLE_ARN, configuration.get(ROLE_ARN_FIELD_NAME));
                jobConf.set(HADOOP_ASSUME_ROLE_EXTERNAL_ID, configuration.get(EXTERNAL_ID_FIELD_NAME));
                jobConf.set(HADOOP_ASSUME_ROLE_SESSION_NAME, "parquet-" + UUID.randomUUID());
                jobConf.set(HADOOP_ASSUME_ROLE_SESSION_DURATION, "15m");

                // TODO: this assumes basic keys always, also support if we use InstanceProfile to assume a role
                jobConf.set(HADOOP_CREDENTIALS_TO_ASSUME_ROLE_KEY, HADOOP_SIMPLE);
                jobConf.set(HADOOP_ACCESS_KEY_ID, configuration.get(ACCESS_KEY_ID_FIELD_NAME));
                jobConf.set(HADOOP_SECRET_ACCESS_KEY, configuration.get(SECRET_ACCESS_KEY_FIELD_NAME));
                break;
            case INSTANCE_PROFILE:
                jobConf.set(HADOOP_CREDENTIAL_PROVIDER_KEY, HADOOP_INSTANCE_PROFILE);
                break;
            case ACCESS_KEYS:
                jobConf.set(HADOOP_CREDENTIAL_PROVIDER_KEY, HADOOP_SIMPLE);
                jobConf.set(HADOOP_ACCESS_KEY_ID, configuration.get(ACCESS_KEY_ID_FIELD_NAME));
                jobConf.set(HADOOP_SECRET_ACCESS_KEY, configuration.get(SECRET_ACCESS_KEY_FIELD_NAME));
                if (configuration.get(SESSION_TOKEN_FIELD_NAME) != null) {
                    jobConf.set(HADOOP_CREDENTIAL_PROVIDER_KEY, HADOOP_SESSION_TOKEN);
                    jobConf.set(HADOOP_SESSION_TOKEN, configuration.get(SESSION_TOKEN_FIELD_NAME));
                }
                break;
            case BAD_AUTHENTICATION:
        }
    }

    /**
     * Validate external dataset properties
     *
     * @param configuration properties
     * @throws CompilationException Compilation exception
     */
    public static void validateProperties(IApplicationContext appCtx, Map<String, String> configuration,
            SourceLocation srcLoc, IWarningCollector collector) throws CompilationException {
        if (isDeltaTable(configuration)) {
            validateDeltaTableProperties(configuration);
        }
        // check if the format property is present
        else if (configuration.get(ExternalDataConstants.KEY_FORMAT) == null) {
            throw new CompilationException(ErrorCode.PARAMETERS_REQUIRED, srcLoc, ExternalDataConstants.KEY_FORMAT);
        }

        String arnRole = configuration.get(ROLE_ARN_FIELD_NAME);
        String externalId = configuration.get(EXTERNAL_ID_FIELD_NAME);
        String accessKeyId = configuration.get(ACCESS_KEY_ID_FIELD_NAME);
        String secretAccessKey = configuration.get(SECRET_ACCESS_KEY_FIELD_NAME);

        if (arnRole != null) {
            return;
        } else if (externalId != null) {
            throw new CompilationException(REQUIRED_PARAM_IF_PARAM_IS_PRESENT, ROLE_ARN_FIELD_NAME,
                    EXTERNAL_ID_FIELD_NAME);
        } else if (accessKeyId == null || secretAccessKey == null) {
            // If one is passed, the other is required
            if (accessKeyId != null) {
                throw new CompilationException(REQUIRED_PARAM_IF_PARAM_IS_PRESENT, SECRET_ACCESS_KEY_FIELD_NAME,
                        ACCESS_KEY_ID_FIELD_NAME);
            } else if (secretAccessKey != null) {
                throw new CompilationException(REQUIRED_PARAM_IF_PARAM_IS_PRESENT, ACCESS_KEY_ID_FIELD_NAME,
                        SECRET_ACCESS_KEY_FIELD_NAME);
            }
        }

        validateIncludeExclude(configuration);
        try {
            // TODO(htowaileb): maybe something better, this will check to ensure type is supported before creation
            new ExternalDataPrefix(configuration);
        } catch (AlgebricksException ex) {
            throw new CompilationException(ErrorCode.FAILED_TO_CALCULATE_COMPUTED_FIELDS, ex);
        }

        // Check if the bucket is present
        S3Client s3Client = buildAwsS3Client(appCtx, configuration);
        S3Response response;
        boolean useOldApi = false;
        String container = configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME);
        String prefix = getPrefix(configuration);

        try {
            response = S3Utils.isBucketEmpty(s3Client, container, prefix, false);
        } catch (S3Exception ex) {
            // Method not implemented, try falling back to old API
            try {
                // For error code, see https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
                if (ex.awsErrorDetails().errorCode().equals(ERROR_METHOD_NOT_IMPLEMENTED)) {
                    useOldApi = true;
                    response = S3Utils.isBucketEmpty(s3Client, container, prefix, true);
                } else {
                    throw ex;
                }
            } catch (SdkException ex2) {
                throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, ex2, getMessageOrToString(ex));
            }
        } catch (SdkException ex) {
            throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, ex, getMessageOrToString(ex));
        } finally {
            if (s3Client != null) {
                CleanupUtils.close(s3Client, null);
            }
        }

        boolean isEmpty = useOldApi ? ((ListObjectsResponse) response).contents().isEmpty()
                : ((ListObjectsV2Response) response).contents().isEmpty();
        if (isEmpty && collector.shouldWarn()) {
            Warning warning = Warning.of(srcLoc, ErrorCode.EXTERNAL_SOURCE_CONFIGURATION_RETURNED_NO_FILES);
            collector.warn(warning);
        }

        // Returns 200 only in case the bucket exists, otherwise, throws an exception. However, to
        // ensure coverage, check if the result is successful as well and not only catch exceptions
        if (!response.sdkHttpResponse().isSuccessful()) {
            throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_CONTAINER_NOT_FOUND, container);
        }
        if (isDeltaTable(configuration)) {
            try {
                validateDeltaTableExists(appCtx, configuration);
            } catch (AlgebricksException e) {
                throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, e);
            }
        }
    }
}
