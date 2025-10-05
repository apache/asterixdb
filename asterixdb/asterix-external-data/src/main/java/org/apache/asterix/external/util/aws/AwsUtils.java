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
package org.apache.asterix.external.util.aws;

import static org.apache.asterix.common.exceptions.ErrorCode.INVALID_PARAM_VALUE_ALLOWED_VALUE;
import static org.apache.asterix.common.exceptions.ErrorCode.PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT;
import static org.apache.asterix.common.exceptions.ErrorCode.REQUIRED_PARAM_IF_PARAM_IS_PRESENT;
import static org.apache.asterix.common.exceptions.ErrorCode.S3_REGION_NOT_SUPPORTED;
import static org.apache.asterix.external.util.aws.AwsConstants.ACCESS_KEY_ID_FIELD_NAME;
import static org.apache.asterix.external.util.aws.AwsConstants.CROSS_REGION_FIELD_NAME;
import static org.apache.asterix.external.util.aws.AwsConstants.ERROR_EXPIRED_TOKEN;
import static org.apache.asterix.external.util.aws.AwsConstants.EXTERNAL_ID_FIELD_NAME;
import static org.apache.asterix.external.util.aws.AwsConstants.INSTANCE_PROFILE_FIELD_NAME;
import static org.apache.asterix.external.util.aws.AwsConstants.REGION_FIELD_NAME;
import static org.apache.asterix.external.util.aws.AwsConstants.ROLE_ARN_FIELD_NAME;
import static org.apache.asterix.external.util.aws.AwsConstants.SECRET_ACCESS_KEY_FIELD_NAME;
import static org.apache.asterix.external.util.aws.AwsConstants.SESSION_TOKEN_FIELD_NAME;
import static org.apache.hyracks.api.util.ExceptionUtils.getMessageOrToString;

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
import org.apache.hyracks.api.exceptions.HyracksDataException;

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

public class AwsUtils {

    public enum AuthenticationType {
        ANONYMOUS,
        ARN_ASSUME_ROLE,
        INSTANCE_PROFILE,
        ACCESS_KEYS,
        BAD_AUTHENTICATION
    }

    private AwsUtils() {
        throw new AssertionError("do not instantiate");
    }

    public static boolean isArnAssumedRoleExpiredToken(Map<String, String> configuration, String errorCode) {
        return ERROR_EXPIRED_TOKEN.equals(errorCode)
                && getAuthenticationType(configuration) == AuthenticationType.ARN_ASSUME_ROLE;
    }

    public static AwsCredentialsProvider buildCredentialsProvider(IApplicationContext appCtx,
            Map<String, String> configuration) throws CompilationException {
        AuthenticationType authenticationType = getAuthenticationType(configuration);
        switch (authenticationType) {
            case ANONYMOUS:
                return AnonymousCredentialsProvider.create();
            case ARN_ASSUME_ROLE:
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

    public static AuthenticationType getAuthenticationType(Map<String, String> configuration) {
        String roleArn = configuration.get(ROLE_ARN_FIELD_NAME);
        String instanceProfile = configuration.get(INSTANCE_PROFILE_FIELD_NAME);
        String accessKeyId = configuration.get(ACCESS_KEY_ID_FIELD_NAME);
        String secretAccessKey = configuration.get(SECRET_ACCESS_KEY_FIELD_NAME);

        if (noAuth(configuration)) {
            return AuthenticationType.ANONYMOUS;
        } else if (roleArn != null) {
            return AuthenticationType.ARN_ASSUME_ROLE;
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
     * Returns the cached credentials if valid, otherwise, generates new credentials
     *
     * @param appCtx application context
     * @param configuration configuration
     * @return returns the cached credentials if valid, otherwise, generates new credentials
     * @throws CompilationException CompilationException
     */
    public static AwsCredentialsProvider getTrustAccountCredentials(IApplicationContext appCtx,
            Map<String, String> configuration) throws CompilationException {
        IExternalCredentialsCache cache = appCtx.getExternalCredentialsCache();
        Object credentialsObject = cache.get(configuration.get(ExternalDataConstants.KEY_ENTITY_ID));
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
        AssumeRoleRequest request = builder.build();
        AwsCredentialsProvider credentialsProvider = getCredentialsToAssumeRole(configuration);

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

    private static AwsCredentialsProvider getCredentialsToAssumeRole(Map<String, String> configuration)
            throws CompilationException {
        String instanceProfile = configuration.get(INSTANCE_PROFILE_FIELD_NAME);
        String accessKeyId = configuration.get(ACCESS_KEY_ID_FIELD_NAME);
        String secretAccessKey = configuration.get(SECRET_ACCESS_KEY_FIELD_NAME);
        if (instanceProfile != null) {
            return getInstanceProfileCredentials(configuration);
        } else if (accessKeyId != null || secretAccessKey != null) {
            return getAccessKeyCredentials(configuration);
        } else {
            throw new CompilationException(ErrorCode.NO_AWS_VALID_PARAMS_FOUND_FOR_CROSS_ACCOUNT_TRUST_AUTHENTICATION);
        }
    }

    private static AwsCredentialsProvider getInstanceProfileCredentials(Map<String, String> configuration)
            throws CompilationException {
        String instanceProfile = configuration.get(INSTANCE_PROFILE_FIELD_NAME);

        // only "true" value is allowed
        if (!"true".equalsIgnoreCase(instanceProfile)) {
            throw new CompilationException(INVALID_PARAM_VALUE_ALLOWED_VALUE, INSTANCE_PROFILE_FIELD_NAME, "true");
        }

        String notAllowed = getNonNull(configuration, ACCESS_KEY_ID_FIELD_NAME, SECRET_ACCESS_KEY_FIELD_NAME,
                SESSION_TOKEN_FIELD_NAME);
        if (notAllowed != null) {
            throw new CompilationException(PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT, notAllowed,
                    INSTANCE_PROFILE_FIELD_NAME);
        }
        return InstanceProfileCredentialsProvider.create();
    }

    private static AwsCredentialsProvider getAccessKeyCredentials(Map<String, String> configuration)
            throws CompilationException {
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

    /**
     * Generates a random external ID to be used in cross-account role assumption.
     *
     * @return external id
     */
    public static String generateExternalId() {
        return UUID.randomUUID().toString();
    }
}
