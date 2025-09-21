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
import static org.apache.asterix.common.exceptions.ErrorCode.LONG_LIVED_CREDENTIALS_NEEDED_TO_ASSUME_ROLE;
import static org.apache.asterix.external.util.ExternalDataUtils.getDisableSslVerify;
import static org.apache.asterix.external.util.ExternalDataUtils.getPrefix;
import static org.apache.asterix.external.util.ExternalDataUtils.isDeltaTable;
import static org.apache.asterix.external.util.ExternalDataUtils.validateDeltaTableExists;
import static org.apache.asterix.external.util.ExternalDataUtils.validateDeltaTableProperties;
import static org.apache.asterix.external.util.ExternalDataUtils.validateIncludeExclude;
import static org.apache.asterix.external.util.aws.AwsConstants.ACCESS_KEY_ID_FIELD_NAME;
import static org.apache.asterix.external.util.aws.AwsConstants.ERROR_INTERNAL_ERROR;
import static org.apache.asterix.external.util.aws.AwsConstants.ERROR_METHOD_NOT_IMPLEMENTED;
import static org.apache.asterix.external.util.aws.AwsConstants.ERROR_SLOW_DOWN;
import static org.apache.asterix.external.util.aws.AwsConstants.EXTERNAL_ID_FIELD_NAME;
import static org.apache.asterix.external.util.aws.AwsConstants.REGION_FIELD_NAME;
import static org.apache.asterix.external.util.aws.AwsConstants.ROLE_ARN_FIELD_NAME;
import static org.apache.asterix.external.util.aws.AwsConstants.SECRET_ACCESS_KEY_FIELD_NAME;
import static org.apache.asterix.external.util.aws.AwsConstants.SERVICE_END_POINT_FIELD_NAME;
import static org.apache.asterix.external.util.aws.AwsConstants.SESSION_TOKEN_FIELD_NAME;
import static org.apache.asterix.external.util.aws.AwsUtils.buildCredentialsProvider;
import static org.apache.asterix.external.util.aws.AwsUtils.buildStsUri;
import static org.apache.asterix.external.util.aws.AwsUtils.getAuthenticationType;
import static org.apache.asterix.external.util.aws.AwsUtils.getCrossRegion;
import static org.apache.asterix.external.util.aws.AwsUtils.getPathStyleAddressing;
import static org.apache.asterix.external.util.aws.AwsUtils.validateAndGetRegion;
import static org.apache.asterix.external.util.aws.s3.S3Constants.FILES;
import static org.apache.asterix.external.util.aws.s3.S3Constants.FOLDERS;
import static org.apache.asterix.external.util.aws.s3.S3Constants.HADOOP_ACCESS_KEY_ID;
import static org.apache.asterix.external.util.aws.s3.S3Constants.HADOOP_ANONYMOUS;
import static org.apache.asterix.external.util.aws.s3.S3Constants.HADOOP_ASSUMED_ROLE;
import static org.apache.asterix.external.util.aws.s3.S3Constants.HADOOP_ASSUME_ROLE_ARN;
import static org.apache.asterix.external.util.aws.s3.S3Constants.HADOOP_ASSUME_ROLE_ENDPOINT;
import static org.apache.asterix.external.util.aws.s3.S3Constants.HADOOP_ASSUME_ROLE_EXTERNAL_ID;
import static org.apache.asterix.external.util.aws.s3.S3Constants.HADOOP_ASSUME_ROLE_REGION;
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
import static org.apache.asterix.external.util.aws.s3.S3Constants.HADOOP_TEMPORARY;
import static org.apache.asterix.external.util.aws.s3.S3Constants.PATH_STYLE_ADDRESSING_FIELD_NAME;
import static org.apache.hyracks.api.util.ExceptionUtils.getMessageOrToString;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
import org.apache.asterix.external.util.aws.AwsUtils;
import org.apache.asterix.external.util.aws.AwsUtils.CloseableAwsClients;
import org.apache.asterix.external.util.iceberg.IcebergUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.S3Response;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;
import software.amazon.awssdk.utils.AttributeMap;

public class S3Utils {
    private static final Logger LOGGER = LogManager.getLogger();

    private S3Utils() {
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

        boolean crossRegion = getCrossRegion(configuration);
        boolean disableSslVerify = getDisableSslVerify(configuration);

        S3ClientBuilder builder = S3Client.builder();
        builder.region(region);
        builder.crossRegionAccessEnabled(crossRegion);
        builder.credentialsProvider(credentialsProvider);
        AwsUtils.setEndpoint(builder, serviceEndpoint);

        boolean pathStyleAddressing = getPathStyleAddressing(configuration);
        builder.forcePathStyle(pathStyleAddressing);
        if (disableSslVerify) {
            disableSslVerify(builder);
        }
        awsClients.setConsumingClient(builder.build());
        return awsClients;

    }

    private static void disableSslVerify(S3ClientBuilder builder) {
        AttributeMap.Builder attributeMapBuilder = AttributeMap.builder();
        attributeMapBuilder.put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true);
        SdkHttpClient httpClient = ApacheHttpClient.builder().buildWithDefaults(attributeMapBuilder.build());
        builder.httpClient(httpClient);
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
        Region region = validateAndGetRegion(configuration.get(REGION_FIELD_NAME));
        boolean crossRegion = getCrossRegion(configuration);

        // if region is set, hadoop will always try the specified region only, if bucket is not found, it will fail
        // if we want to use cross-region, we do not set the endpoint, which will default to central region and will
        // automatically detect the bucket
        if (!crossRegion) {
            jobConf.set(HADOOP_REGION, region.toString());
        }
        setHadoopCredentials(appCtx, jobConf, configuration, region.toString());

        String serviceEndpoint = configuration.get(SERVICE_END_POINT_FIELD_NAME);
        if (serviceEndpoint != null) {
            // Validation of the URL should be done at hadoop-aws level
            jobConf.set(HADOOP_SERVICE_END_POINT, serviceEndpoint);
        }

        boolean pathStyleAddressing =
                validateAndGetPathStyleAddressing(configuration.get(PATH_STYLE_ADDRESSING_FIELD_NAME), serviceEndpoint);
        if (pathStyleAddressing) {
            jobConf.set(HADOOP_PATH_STYLE_ACCESS, ExternalDataConstants.TRUE);
        } else {
            jobConf.set(HADOOP_PATH_STYLE_ACCESS, ExternalDataConstants.FALSE);
        }

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
    private static void setHadoopCredentials(IApplicationContext appCtx, JobConf jobConf,
            Map<String, String> configuration, String region) throws CompilationException {
        AwsUtils.AuthenticationType authenticationType = getAuthenticationType(configuration);
        switch (authenticationType) {
            case ANONYMOUS:
                jobConf.set(HADOOP_CREDENTIAL_PROVIDER_KEY, HADOOP_ANONYMOUS);
                break;
            case ARN_ASSUME_ROLE:
                jobConf.set(HADOOP_CREDENTIAL_PROVIDER_KEY, HADOOP_ASSUMED_ROLE);
                jobConf.set(HADOOP_ASSUME_ROLE_EXTERNAL_ID, configuration.get(EXTERNAL_ID_FIELD_NAME));
                jobConf.set(HADOOP_ASSUME_ROLE_REGION, region);
                jobConf.set(HADOOP_ASSUME_ROLE_ENDPOINT, buildStsUri(region));

                String roleArn = configuration.get(ROLE_ARN_FIELD_NAME);
                String sessionName = UUID.randomUUID().toString();
                jobConf.set(HADOOP_ASSUME_ROLE_ARN, roleArn);
                jobConf.set(HADOOP_ASSUME_ROLE_SESSION_NAME, sessionName);
                LOGGER.debug("Assuming RoleArn ({}) and SessionName ({})", roleArn, sessionName);

                // hadoop accepts time 15m to 1h, we will base it on the provided configuration
                int durationInSeconds = appCtx.getExternalProperties().getAwsAssumeRoleDuration();
                String hadoopDuration = getHadoopDuration(durationInSeconds);
                jobConf.set(HADOOP_ASSUME_ROLE_SESSION_DURATION, hadoopDuration);

                // credentials used to assume the role
                AwsUtils.AuthenticationType assumeRoleCredsType = AwsUtils.getAuthenticationType(configuration, true);
                switch (assumeRoleCredsType) {
                    case INSTANCE_PROFILE:
                        jobConf.set(HADOOP_CREDENTIALS_TO_ASSUME_ROLE_KEY, HADOOP_INSTANCE_PROFILE);
                        break;
                    case ACCESS_KEYS:
                        jobConf.set(HADOOP_CREDENTIALS_TO_ASSUME_ROLE_KEY, HADOOP_SIMPLE);
                        jobConf.set(HADOOP_ACCESS_KEY_ID, configuration.get(ACCESS_KEY_ID_FIELD_NAME));
                        jobConf.set(HADOOP_SECRET_ACCESS_KEY, configuration.get(SECRET_ACCESS_KEY_FIELD_NAME));
                        break;
                    default:
                        throw CompilationException.create(LONG_LIVED_CREDENTIALS_NEEDED_TO_ASSUME_ROLE);
                }
                break;
            case INSTANCE_PROFILE:
                jobConf.set(HADOOP_CREDENTIAL_PROVIDER_KEY, HADOOP_INSTANCE_PROFILE);
                break;
            case ACCESS_KEYS:
                jobConf.set(HADOOP_CREDENTIAL_PROVIDER_KEY, HADOOP_SIMPLE);
                jobConf.set(HADOOP_ACCESS_KEY_ID, configuration.get(ACCESS_KEY_ID_FIELD_NAME));
                jobConf.set(HADOOP_SECRET_ACCESS_KEY, configuration.get(SECRET_ACCESS_KEY_FIELD_NAME));
                if (configuration.get(SESSION_TOKEN_FIELD_NAME) != null) {
                    jobConf.set(HADOOP_CREDENTIAL_PROVIDER_KEY, HADOOP_TEMPORARY);
                    jobConf.set(HADOOP_SESSION_TOKEN, configuration.get(SESSION_TOKEN_FIELD_NAME));
                }
                break;
            case BAD_AUTHENTICATION:
                throw new CompilationException(ErrorCode.NO_VALID_AUTHENTICATION_PARAMS_PROVIDED);
        }
    }

    /**
     * Hadoop accepts duration values from 15m to 1h (in this format). We will base this on the configured
     * duration in seconds. If the time exceeds 1 hour, we will return 1h
     *
     * @param seconds configured duration in seconds
     * @return hadoop updated duration
     */
    private static String getHadoopDuration(int seconds) {
        // constants for time thresholds
        final int FIFTEEN_MINUTES_IN_SECONDS = 15 * 60;
        final int ONE_HOUR_IN_SECONDS = 60 * 60;

        // Adjust seconds to fit within bounds
        if (seconds < FIFTEEN_MINUTES_IN_SECONDS) {
            seconds = FIFTEEN_MINUTES_IN_SECONDS;
        } else if (seconds > ONE_HOUR_IN_SECONDS) {
            seconds = ONE_HOUR_IN_SECONDS;
        }

        // Convert seconds to minutes
        int minutes = seconds / 60;

        // Format the result
        if (minutes == 60) {
            return "1h";
        } else {
            return minutes + "m";
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

        // iceberg tables can be created without passing the bucket,
        // only validate bucket presence if container is passed
        String container = configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME);
        if (IcebergUtils.isIcebergTable(configuration) && container == null) {
            return;
        }

        validateIncludeExclude(configuration);
        try {
            // TODO(htowaileb): maybe something better, this will check to ensure type is supported before creation
            new ExternalDataPrefix(configuration);
        } catch (AlgebricksException ex) {
            throw new CompilationException(ErrorCode.FAILED_TO_CALCULATE_COMPUTED_FIELDS, ex);
        }

        // Check if the bucket is present
        CloseableAwsClients awsClients = buildClient(appCtx, configuration);
        S3Client s3Client = (S3Client) awsClients.getConsumingClient();
        S3Response response;
        boolean useOldApi = false;
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
            AwsUtils.closeClients(awsClients);
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

    /**
     * Checks for a single object in the specified bucket to determine if the bucket is empty or not.
     *
     * @param s3Client  s3 client
     * @param container the container name
     * @param prefix    Prefix to be used
     * @param useOldApi flag whether to use the old API or not
     * @return returns the S3 response
     */
    protected static S3Response isBucketEmpty(S3Client s3Client, String container, String prefix, boolean useOldApi) {
        S3Response response;
        if (useOldApi) {
            ListObjectsRequest.Builder listObjectsBuilder = ListObjectsRequest.builder();
            listObjectsBuilder.prefix(prefix);
            response = s3Client.listObjects(listObjectsBuilder.bucket(container).maxKeys(1).build());
        } else {
            ListObjectsV2Request.Builder listObjectsBuilder = ListObjectsV2Request.builder();
            listObjectsBuilder.prefix(prefix);
            response = s3Client.listObjectsV2(listObjectsBuilder.bucket(container).maxKeys(1).build());
        }
        return response;
    }

    /**
     * Returns the lists of S3 objects.
     *
     * @param configuration         properties
     * @param includeExcludeMatcher include/exclude matchers to apply
     */
    public static List<S3Object> listS3Objects(IApplicationContext appCtx, Map<String, String> configuration,
            AbstractExternalInputStreamFactory.IncludeExcludeMatcher includeExcludeMatcher,
            IWarningCollector warningCollector, ExternalDataPrefix externalDataPrefix,
            IExternalFilterEvaluator evaluator) throws CompilationException, HyracksDataException {
        // Prepare to retrieve the objects
        List<S3Object> filesOnly;
        String container = configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME);
        CloseableAwsClients awsClients = buildClient(appCtx, configuration);
        S3Client s3Client = (S3Client) awsClients.getConsumingClient();
        String prefix = getPrefix(configuration);

        try {
            filesOnly = listS3Objects(s3Client, container, prefix, includeExcludeMatcher, externalDataPrefix, evaluator,
                    warningCollector);
        } catch (S3Exception ex) {
            // New API is not implemented, try falling back to old API
            try {
                // For error code, see https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
                if (ex.awsErrorDetails().errorCode().equals(ERROR_METHOD_NOT_IMPLEMENTED)) {
                    filesOnly = oldApiListS3Objects(s3Client, container, prefix, includeExcludeMatcher,
                            externalDataPrefix, evaluator, warningCollector);
                } else {
                    throw ex;
                }
            } catch (SdkException ex2) {
                throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, ex2, getMessageOrToString(ex));
            }
        } catch (SdkException ex) {
            throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, ex, getMessageOrToString(ex));
        } finally {
            AwsUtils.closeClients(awsClients);
        }

        // Warn if no files are returned
        if (filesOnly.isEmpty() && warningCollector.shouldWarn()) {
            Warning warning = Warning.of(null, ErrorCode.EXTERNAL_SOURCE_CONFIGURATION_RETURNED_NO_FILES);
            warningCollector.warn(warning);
        }

        return filesOnly;
    }

    /**
     * Uses the latest API to retrieve the objects from the storage.
     *
     * @param s3Client              S3 client
     * @param container             container name
     * @param prefix                definition prefix
     * @param includeExcludeMatcher include/exclude matchers to apply
     */
    private static List<S3Object> listS3Objects(S3Client s3Client, String container, String prefix,
            AbstractExternalInputStreamFactory.IncludeExcludeMatcher includeExcludeMatcher,
            ExternalDataPrefix externalDataPrefix, IExternalFilterEvaluator evaluator,
            IWarningCollector warningCollector) throws HyracksDataException {
        String newMarker = null;
        List<S3Object> filesOnly = new ArrayList<>();

        ListObjectsV2Response listObjectsResponse;
        ListObjectsV2Request.Builder listObjectsBuilder = ListObjectsV2Request.builder().bucket(container);
        listObjectsBuilder.prefix(prefix);

        while (true) {
            // List the objects from the start, or from the last marker in case of truncated result
            if (newMarker == null) {
                listObjectsResponse = s3Client.listObjectsV2(listObjectsBuilder.build());
            } else {
                listObjectsResponse = s3Client.listObjectsV2(listObjectsBuilder.continuationToken(newMarker).build());
            }

            // Collect the paths to files only
            collectAndFilterFiles(listObjectsResponse.contents(), includeExcludeMatcher.getPredicate(),
                    includeExcludeMatcher.getMatchersList(), filesOnly, externalDataPrefix, evaluator,
                    warningCollector);

            // Mark the flag as done if done, otherwise, get the marker of the previous response for the next request
            if (listObjectsResponse.isTruncated() != null && listObjectsResponse.isTruncated()) {
                newMarker = listObjectsResponse.nextContinuationToken();
            } else {
                break;
            }
        }

        return filesOnly;
    }

    /**
     * Uses the old API (in case the new API is not implemented) to retrieve the objects from the storage
     *
     * @param s3Client              S3 client
     * @param container             container name
     * @param prefix                definition prefix
     * @param includeExcludeMatcher include/exclude matchers to apply
     */
    private static List<S3Object> oldApiListS3Objects(S3Client s3Client, String container, String prefix,
            AbstractExternalInputStreamFactory.IncludeExcludeMatcher includeExcludeMatcher,
            ExternalDataPrefix externalDataPrefix, IExternalFilterEvaluator evaluator,
            IWarningCollector warningCollector) throws HyracksDataException {
        String newMarker = null;
        List<S3Object> filesOnly = new ArrayList<>();

        ListObjectsResponse listObjectsResponse;
        ListObjectsRequest.Builder listObjectsBuilder = ListObjectsRequest.builder().bucket(container);
        listObjectsBuilder.prefix(prefix);

        while (true) {
            // List the objects from the start, or from the last marker in case of truncated result
            if (newMarker == null) {
                listObjectsResponse = s3Client.listObjects(listObjectsBuilder.build());
            } else {
                listObjectsResponse = s3Client.listObjects(listObjectsBuilder.marker(newMarker).build());
            }

            // Collect the paths to files only
            collectAndFilterFiles(listObjectsResponse.contents(), includeExcludeMatcher.getPredicate(),
                    includeExcludeMatcher.getMatchersList(), filesOnly, externalDataPrefix, evaluator,
                    warningCollector);

            // Mark the flag as done if done, otherwise, get the marker of the previous response for the next request
            if (listObjectsResponse.isTruncated() != null && listObjectsResponse.isTruncated()) {
                newMarker = listObjectsResponse.nextMarker();
            } else {
                break;
            }
        }

        return filesOnly;
    }

    /**
     * Collects only files that pass all tests
     *
     * @param s3Objects          s3 objects
     * @param predicate          predicate
     * @param matchers           matchers
     * @param filesOnly          filtered files
     * @param externalDataPrefix external data prefix
     * @param evaluator          evaluator
     */
    private static void collectAndFilterFiles(List<S3Object> s3Objects, BiPredicate<List<Matcher>, String> predicate,
            List<Matcher> matchers, List<S3Object> filesOnly, ExternalDataPrefix externalDataPrefix,
            IExternalFilterEvaluator evaluator, IWarningCollector warningCollector) throws HyracksDataException {
        for (S3Object object : s3Objects) {
            if (ExternalDataUtils.evaluate(object.key(), predicate, matchers, externalDataPrefix, evaluator,
                    warningCollector)) {
                filesOnly.add(object);
            }
        }
    }

    /**
     * Uses the latest API to retrieve the objects from the storage of a single level.
     *
     * @param appCtx application context
     * @param configuration configuration
     * @param container container name
     * @param prefix definition prefix
     */
    public static Map<String, List<String>> listS3ObjectsOfSingleDepth(IApplicationContext appCtx,
            Map<String, String> configuration, String container, String prefix) throws CompilationException {
        CloseableAwsClients awsClients = buildClient(appCtx, configuration);
        S3Client s3Client = (S3Client) awsClients.getConsumingClient();

        ListObjectsV2Request.Builder listObjectsBuilder = ListObjectsV2Request.builder();
        listObjectsBuilder.bucket(container);
        listObjectsBuilder.prefix(prefix);
        listObjectsBuilder.delimiter("/");
        ListObjectsV2Request listObjectsV2Request = listObjectsBuilder.build();

        Map<String, List<String>> allObjects = new HashMap<>();
        List<String> files = new ArrayList<>();
        List<String> folders = new ArrayList<>();

        // to skip the prefix as a file from the response
        boolean checkPrefixInFile = true;
        try {
            ListObjectsV2Iterable listObjectsIterable = s3Client.listObjectsV2Paginator(listObjectsV2Request);
            for (ListObjectsV2Response response : listObjectsIterable) {
                // put all the files
                for (S3Object object : response.contents()) {
                    String fileName = object.key();
                    fileName = fileName.substring(prefix.length());
                    if (checkPrefixInFile && prefix.equals(object.key())) {
                        checkPrefixInFile = false;
                    } else {
                        files.add(fileName);
                    }
                }

                // put all the folders
                for (CommonPrefix object : response.commonPrefixes()) {
                    String folderName = object.prefix();
                    folderName = folderName.substring(prefix.length());
                    folders.add(
                            folderName.endsWith("/") ? folderName.substring(0, folderName.length() - 1) : folderName);
                }
            }
        } finally {
            AwsUtils.closeClients(awsClients);
        }

        allObjects.put(FILES, files);
        allObjects.put(FOLDERS, folders);
        return allObjects;
    }

    public static String getPath(Map<String, String> configuration) {
        return S3Constants.HADOOP_S3_PROTOCOL + "://"
                + configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME) + '/'
                + configuration.get(ExternalDataConstants.DEFINITION_FIELD_NAME);
    }

    public static boolean isRetryableError(String errorCode) {
        return errorCode.equals(ERROR_INTERNAL_ERROR) || errorCode.equals(ERROR_SLOW_DOWN);
    }

    public static boolean validateAndGetPathStyleAddressing(String pathStyleAddressing, String endpoint)
            throws CompilationException {
        if (pathStyleAddressing == null) {
            return endpoint != null && !endpoint.isEmpty();
        }
        validatePathStyleAddressing(pathStyleAddressing);
        return "true".equalsIgnoreCase(pathStyleAddressing);
    }

    public static void validatePathStyleAddressing(String pathStyleAddressing) throws CompilationException {
        if (pathStyleAddressing == null) {
            return;
        }
        if (!"true".equalsIgnoreCase(pathStyleAddressing) && !"false".equalsIgnoreCase(pathStyleAddressing)) {
            throw new CompilationException(INVALID_PARAM_VALUE_ALLOWED_VALUE, PATH_STYLE_ADDRESSING_FIELD_NAME,
                    "true, false");
        }
    }
}
