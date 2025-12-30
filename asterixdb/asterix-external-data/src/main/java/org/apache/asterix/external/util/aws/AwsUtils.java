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
import static org.apache.asterix.external.util.ExternalDataUtils.validateAndGetBooleanProperty;
import static org.apache.asterix.external.util.aws.AwsConstants.ACCESS_KEY_ID_FIELD_NAME;
import static org.apache.asterix.external.util.aws.AwsConstants.CROSS_REGION_FIELD_NAME;
import static org.apache.asterix.external.util.aws.AwsConstants.EXTERNAL_ID_FIELD_NAME;
import static org.apache.asterix.external.util.aws.AwsConstants.INSTANCE_PROFILE_FIELD_NAME;
import static org.apache.asterix.external.util.aws.AwsConstants.REGION_FIELD_NAME;
import static org.apache.asterix.external.util.aws.AwsConstants.ROLE_ARN_FIELD_NAME;
import static org.apache.asterix.external.util.aws.AwsConstants.SECRET_ACCESS_KEY_FIELD_NAME;
import static org.apache.asterix.external.util.aws.AwsConstants.SESSION_TOKEN_FIELD_NAME;
import static org.apache.asterix.external.util.aws.s3.S3Constants.PATH_STYLE_ADDRESSING_FIELD_NAME;
import static org.apache.hyracks.api.util.ExceptionUtils.getMessageOrToString;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.external.IExternalStatsTracker;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.AwsClient;
import software.amazon.awssdk.core.SdkClient;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.SdkResponse;
import software.amazon.awssdk.core.client.builder.SdkClientBuilder;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;

public class AwsUtils {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final ExecutionInterceptor ASSUME_ROLE_INTERCEPTOR;

    static {
        ASSUME_ROLE_INTERCEPTOR = new ExecutionInterceptor() {
            @Override
            public void afterExecution(Context.AfterExecution context, ExecutionAttributes executionAttributes) {
                SdkRequest req = context.request();
                SdkResponse resp = context.response();
                if (req instanceof AssumeRoleRequest assumeReq && resp instanceof AssumeRoleResponse assumeResp) {
                    LOGGER.debug("STS refresh success [Role={}, Session={}, Expiry={}]",
                            assumeReq.roleArn(),
                            assumeReq.roleSessionName(),
                            assumeResp.credentials().expiration());
                }
                ExecutionInterceptor.super.afterExecution(context, executionAttributes);
            }
        };
    }

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

    public static AwsCredentialsProvider buildCredentialsProvider(IApplicationContext appCtx,
            Map<String, String> configuration, CloseableAwsClients awsClients) throws CompilationException {
        AuthenticationType authenticationType = getAuthenticationType(configuration);
        switch (authenticationType) {
            case ANONYMOUS:
                return AnonymousCredentialsProvider.create();
            case ARN_ASSUME_ROLE:
                return getTrustAccountCredentials(appCtx, configuration, awsClients);
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
        return getAuthenticationType(configuration, false);
    }

    public static AuthenticationType getAuthenticationType(Map<String, String> configuration,
            boolean credentialsToAssumeRole) {
        String roleArn = configuration.get(ROLE_ARN_FIELD_NAME);
        String instanceProfile = configuration.get(INSTANCE_PROFILE_FIELD_NAME);
        String accessKeyId = configuration.get(ACCESS_KEY_ID_FIELD_NAME);
        String secretAccessKey = configuration.get(SECRET_ACCESS_KEY_FIELD_NAME);

        if (noAuth(configuration)) {
            return AuthenticationType.ANONYMOUS;
        } else if (roleArn != null && !credentialsToAssumeRole) {
            return AuthenticationType.ARN_ASSUME_ROLE;
        } else if (instanceProfile != null) {
            return AuthenticationType.INSTANCE_PROFILE;
        } else if (accessKeyId != null || secretAccessKey != null) {
            return AuthenticationType.ACCESS_KEYS;
        } else {
            return AuthenticationType.BAD_AUTHENTICATION;
        }
    }

    public static boolean getCrossRegion(Map<String, String> configuration) throws CompilationException {
        String crossRegionString = configuration.get(CROSS_REGION_FIELD_NAME);
        return validateAndGetBooleanProperty(CROSS_REGION_FIELD_NAME, crossRegionString);
    }

    public static boolean getPathStyleAddressing(Map<String, String> configuration) throws CompilationException {
        String pathStyleAccessString = configuration.get(PATH_STYLE_ADDRESSING_FIELD_NAME);
        return validateAndGetBooleanProperty(PATH_STYLE_ADDRESSING_FIELD_NAME, pathStyleAccessString);
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
            Map<String, String> configuration, CloseableAwsClients awsClients) throws CompilationException {
        AwsCredentialsProvider credentialsToAssumeRole = getCredentialsToAssumeRole(configuration);

        // build sts client used for assuming role
        ClientOverrideConfiguration.Builder clientConfigurationBuilder = ClientOverrideConfiguration.builder();
        clientConfigurationBuilder.addExecutionInterceptor(ASSUME_ROLE_INTERCEPTOR);
        if (appCtx != null) {
            clientConfigurationBuilder.addExecutionInterceptor(new ExecutionInterceptor() {
                @Override
                public void onExecutionFailure(Context.FailedExecution context, ExecutionAttributes executionAttributes) {
                    SdkRequest req = context.request();
                    if (req instanceof AssumeRoleRequest assumeReq) {
                        String roleArn = assumeReq.roleArn();
                        Throwable th = context.exception();
                        LOGGER.info("encountered issue assuming role ({}): {}", roleArn, getMessageOrToString(th));

                        IExternalStatsTracker externalStatsTracker = appCtx.getExternalStatsTracker();
                        String name = externalStatsTracker.resolveName(configuration);
                        externalStatsTracker.incrementAwsAssumeRoleFailure(name, roleArn);
                    }
                    ExecutionInterceptor.super.onExecutionFailure(context, executionAttributes);
                }
            });
        }
        ClientOverrideConfiguration clientConfiguration = clientConfigurationBuilder.build();

        StsClientBuilder stsClientBuilder = StsClient.builder();
        stsClientBuilder.credentialsProvider(credentialsToAssumeRole);
        stsClientBuilder.region(validateAndGetRegion(configuration.get(REGION_FIELD_NAME)));
        stsClientBuilder.overrideConfiguration(clientConfiguration);
        StsClient stsClient = stsClientBuilder.build();
        awsClients.setStsClient(stsClient);

        // build refresh role request
        String roleArn = configuration.get(ROLE_ARN_FIELD_NAME);
        String sessionName = UUID.randomUUID().toString();
        LOGGER.debug("Assuming RoleArn ({}) and SessionName ({})", roleArn, sessionName);
        AssumeRoleRequest.Builder refreshRequestBuilder = AssumeRoleRequest.builder();
        refreshRequestBuilder.roleArn(roleArn);
        refreshRequestBuilder.externalId(configuration.get(EXTERNAL_ID_FIELD_NAME));
        refreshRequestBuilder.roleSessionName(sessionName);
        int duration = AwsConstants.ASSUME_ROLE_DURATION_DEFAULT;
        if (appCtx != null) {
            duration = appCtx.getExternalProperties().getAwsAssumeRoleDuration();
        }
        refreshRequestBuilder.durationSeconds(duration);

        // build credentials provider
        StsAssumeRoleCredentialsProvider.Builder builder = StsAssumeRoleCredentialsProvider.builder();
        builder.refreshRequest(refreshRequestBuilder.build());
        builder.stsClient(stsClient);
        if (appCtx != null) {
            int staleTime = appCtx.getExternalProperties().getAwsAssumeRoleStaleTime();
            int prefetchTime = appCtx.getExternalProperties().getAwsAssumeRolePrefetchTime();
            boolean asyncCredentialsUpdate = appCtx.getExternalProperties().getAwsAssumeRoleAsyncRefreshEnabled();
            builder.staleTime(Duration.ofSeconds(staleTime));
            builder.prefetchTime(Duration.ofSeconds(prefetchTime));
            builder.asyncCredentialUpdateEnabled(asyncCredentialsUpdate);
        }

        StsAssumeRoleCredentialsProvider credentialsProvider = builder.build();
        awsClients.setCredentialsProvider(credentialsProvider);
        return credentialsProvider;
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

    public static <B extends SdkClientBuilder<B, C>, C extends SdkClient> void setEndpoint(B builder, String endpoint)
            throws CompilationException {
        // Validate the service endpoint if present
        if (endpoint != null) {
            try {
                URI uri = new URI(endpoint);
                try {
                    builder.endpointOverride(uri);
                } catch (NullPointerException ex) {
                    throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, ex, getMessageOrToString(ex));
                }
            } catch (URISyntaxException ex) {
                throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, ex,
                        String.format("Invalid service endpoint %s", endpoint));
            }
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

    public static String buildStsUri(String regionId) {
        // hadoop expects the endpoint without scheme, it automatically prepends "https://"
        return "sts." + regionId + ".amazonaws.com";
    }

    public static void closeClients(CloseableAwsClients clients) {
        if (clients == null) {
            return;
        }

        AwsCredentialsProvider credentialsProvider = clients.getCredentialsProvider();
        if (credentialsProvider instanceof StsAssumeRoleCredentialsProvider assumeRoleCredsProvider) {
            CleanupUtils.nonThrowingClose(null, clients.getConsumingClient(), clients.getStsClient(), assumeRoleCredsProvider);
        } else {
            CleanupUtils.nonThrowingClose(null, clients.getConsumingClient(), clients.getStsClient());
        }
    }

    public static class CloseableAwsClients {
        private AwsClient consumingClient;
        private StsClient stsClient;
        private AwsCredentialsProvider credentialsProvider;

        public CloseableAwsClients() {

        }

        public AwsClient getConsumingClient() {
            return consumingClient;
        }

        public StsClient getStsClient() {
            return stsClient;
        }

        public AwsCredentialsProvider getCredentialsProvider() {
            return credentialsProvider;
        }

        public void setConsumingClient(AwsClient consumingClient) {
            this.consumingClient = consumingClient;
        }

        public void setStsClient(StsClient stsClient) {
            this.stsClient = stsClient;
        }

        public void setCredentialsProvider(AwsCredentialsProvider credentialsProvider) {
            this.credentialsProvider = credentialsProvider;
        }
    }
}
