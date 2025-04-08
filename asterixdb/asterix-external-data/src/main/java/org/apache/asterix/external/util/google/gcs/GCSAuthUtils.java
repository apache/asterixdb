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
package org.apache.asterix.external.util.google.gcs;

import static org.apache.asterix.common.exceptions.ErrorCode.EXTERNAL_SOURCE_ERROR;
import static org.apache.asterix.common.exceptions.ErrorCode.PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT;
import static org.apache.asterix.external.util.google.gcs.GCSConstants.APPLICATION_DEFAULT_CREDENTIALS_FIELD_NAME;
import static org.apache.asterix.external.util.google.gcs.GCSConstants.ENDPOINT_FIELD_NAME;
import static org.apache.asterix.external.util.google.gcs.GCSConstants.HADOOP_AUTH_TYPE;
import static org.apache.asterix.external.util.google.gcs.GCSConstants.HADOOP_AUTH_UNAUTHENTICATED;
import static org.apache.asterix.external.util.google.gcs.GCSConstants.HADOOP_ENDPOINT;
import static org.apache.asterix.external.util.google.gcs.GCSConstants.HADOOP_GCS_PROTOCOL;
import static org.apache.asterix.external.util.google.gcs.GCSConstants.IMPERSONATE_SERVICE_ACCOUNT_FIELD_NAME;
import static org.apache.asterix.external.util.google.gcs.GCSConstants.JSON_CREDENTIALS_FIELD_NAME;
import static org.apache.asterix.external.util.google.gcs.GCSConstants.HadoopAuthServiceAccount.IMPERSONATE_SERVICE_ACCOUNT;
import static org.apache.hyracks.api.util.ExceptionUtils.getMessageOrToString;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.HDFSUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

public class GCSAuthUtils {
    enum AuthenticationType {
        ANONYMOUS,
        IMPERSONATE_SERVICE_ACCOUNT,
        APPLICATION_DEFAULT_CREDENTIALS,
        SERVICE_ACCOUNT_KEY_JSON_CREDENTIALS,
        BAD_AUTHENTICATION
    }

    private static final ObjectMapper JSON_CREDENTIALS_OBJECT_MAPPER = new ObjectMapper();
    private static final List<String> READ_WRITE_SCOPE_PERMISSION =
            Collections.singletonList("https://www.googleapis.com/auth/devstorage.read_write");
    static {
        JSON_CREDENTIALS_OBJECT_MAPPER.configure(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature(), true);
    }

    private GCSAuthUtils() {
        throw new AssertionError("do not instantiate");
    }

    /**
     * Builds the client using the provided configuration
     *
     * @param appCtx application context
     * @param configuration properties
     * @return Storage client
     * @throws CompilationException CompilationException
     */
    public static Storage buildClient(IApplicationContext appCtx, Map<String, String> configuration)
            throws CompilationException {
        String endpoint = configuration.get(ENDPOINT_FIELD_NAME);

        Credentials credentials = buildCredentials(appCtx, configuration);
        StorageOptions.Builder builder = StorageOptions.newBuilder();
        builder.setCredentials(credentials);

        if (endpoint != null) {
            builder.setHost(endpoint);
        }

        return builder.build().getService();
    }

    public static Credentials buildCredentials(IApplicationContext appCtx, Map<String, String> configuration) throws CompilationException {
        AuthenticationType authenticationType = getAuthenticationType(configuration);
        return switch (authenticationType) {
            case ANONYMOUS -> NoCredentials.getInstance();
            case IMPERSONATE_SERVICE_ACCOUNT -> getImpersonatedServiceAccountCredentials(appCtx, configuration);
            case APPLICATION_DEFAULT_CREDENTIALS -> getApplicationDefaultCredentials(configuration);
            case SERVICE_ACCOUNT_KEY_JSON_CREDENTIALS -> getServiceAccountKeyCredentials(configuration);
            case BAD_AUTHENTICATION -> throw new CompilationException(ErrorCode.NO_VALID_AUTHENTICATION_PARAMS_PROVIDED);
        };
    }

    private static AuthenticationType getAuthenticationType(Map<String, String> configuration) {
        String impersonateServiceAccount = configuration.get(IMPERSONATE_SERVICE_ACCOUNT_FIELD_NAME);
        String applicationDefaultCredentials = configuration.get(APPLICATION_DEFAULT_CREDENTIALS_FIELD_NAME);
        String jsonCredentials = configuration.get(JSON_CREDENTIALS_FIELD_NAME);

        if (noAuth(configuration)) {
            return AuthenticationType.ANONYMOUS;
        } else if (impersonateServiceAccount != null) {
            return AuthenticationType.IMPERSONATE_SERVICE_ACCOUNT;
        } else if (applicationDefaultCredentials != null) {
            return AuthenticationType.APPLICATION_DEFAULT_CREDENTIALS;
        } else if (jsonCredentials != null) {
            return AuthenticationType.SERVICE_ACCOUNT_KEY_JSON_CREDENTIALS;
        } else {
            return AuthenticationType.BAD_AUTHENTICATION;
        }
    }

    private static boolean noAuth(Map<String, String> configuration) {
        return getNonNull(configuration, APPLICATION_DEFAULT_CREDENTIALS_FIELD_NAME, JSON_CREDENTIALS_FIELD_NAME,
                IMPERSONATE_SERVICE_ACCOUNT_FIELD_NAME) == null;
    }

    /**
     * Returns the cached credentials if valid, otherwise, generates new credentials
     *
     * @param appCtx application context
     * @param configuration configuration
     * @return returns the cached credentials if valid, otherwise, generates new credentials
     * @throws CompilationException CompilationException
     */
    public static GoogleCredentials getImpersonatedServiceAccountCredentials(IApplicationContext appCtx,
            Map<String, String> configuration) throws CompilationException {
        GoogleCredentials sourceCredentials = getCredentialsToImpersonateServiceAccount(configuration);
        String impersonateServiceAccount = configuration.get(IMPERSONATE_SERVICE_ACCOUNT_FIELD_NAME);
        int duration = appCtx.getExternalProperties().getGcpImpersonateServiceAccountDuration();

        // Create impersonated credentials
        return ImpersonatedCredentials.create(sourceCredentials, impersonateServiceAccount, null,
                READ_WRITE_SCOPE_PERMISSION, duration);
    }

    private static GoogleCredentials getCredentialsToImpersonateServiceAccount(Map<String, String> configuration)
            throws CompilationException {
        String applicationDefaultCredentials = configuration.get(APPLICATION_DEFAULT_CREDENTIALS_FIELD_NAME);
        String jsonCredentials = configuration.get(JSON_CREDENTIALS_FIELD_NAME);

        if (applicationDefaultCredentials != null) {
            return getApplicationDefaultCredentials(configuration);
        } else if (jsonCredentials != null) {
            return getServiceAccountKeyCredentials(configuration);
        } else {
            throw new CompilationException(
                    ErrorCode.NO_VALID_AUTHENTICATION_PARAMS_PROVIDED_TO_IMPERSONATE_SERVICE_ACCOUNT);
        }
    }

    private static GoogleCredentials getApplicationDefaultCredentials(Map<String, String> configuration)
            throws CompilationException {
        try {
            String notAllowed = getNonNull(configuration, JSON_CREDENTIALS_FIELD_NAME);
            if (notAllowed != null) {
                throw new CompilationException(PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT, notAllowed,
                        APPLICATION_DEFAULT_CREDENTIALS_FIELD_NAME);
            }
            return GoogleCredentials.getApplicationDefault();
        } catch (Exception ex) {
            throw CompilationException.create(EXTERNAL_SOURCE_ERROR, ex, getMessageOrToString(ex));
        }
    }

    private static GoogleCredentials getServiceAccountKeyCredentials(Map<String, String> configuration)
            throws CompilationException {
        String jsonCredentials = configuration.get(JSON_CREDENTIALS_FIELD_NAME);
        try (InputStream credentialsStream = new ByteArrayInputStream(jsonCredentials.getBytes())) {
            return GoogleCredentials.fromStream(credentialsStream);
        } catch (IOException ex) {
            throw CompilationException.create(EXTERNAL_SOURCE_ERROR, ex, getMessageOrToString(ex));
        } catch (Exception ex) {
            throw CompilationException.create(EXTERNAL_SOURCE_ERROR, ex,
                    "Encountered an issue while processing the JSON credentials. Please ensure the provided credentials are valid.");
        }
    }

    public static void configureHdfsJobConf(JobConf jobConf, Map<String, String> configuration)
            throws AlgebricksException {
        configureHdfsJobConf(jobConf, configuration, 0);
    }

    /**
     * Builds the client using the provided configuration
     *
     * @param configuration      properties
     * @param numberOfPartitions number of partitions
     */
    public static void configureHdfsJobConf(JobConf jobConf, Map<String, String> configuration, int numberOfPartitions)
            throws AlgebricksException {
        setHadoopCredentials(jobConf, configuration);

        // set endpoint if provided, default is https://storage.googleapis.com/
        String endpoint = configuration.get(ENDPOINT_FIELD_NAME);
        if (endpoint != null) {
            jobConf.set(HADOOP_ENDPOINT, endpoint);
        }

        // disable caching FileSystem
        HDFSUtils.disableHadoopFileSystemCache(jobConf, HADOOP_GCS_PROTOCOL);

        // TODO(htowaileb): make configurable, in case we hit rate limits then we can reduce it, default is 15
        if (numberOfPartitions != 0) {
            jobConf.set(GCSConstants.MAX_BATCH_THREADS, String.valueOf(numberOfPartitions));
        }

        // recommended to be disabled by GCP hadoop team
        jobConf.set(GCSConstants.HADOOP_SUPPORT_COMPRESSED, ExternalDataConstants.FALSE);
    }

    /**
     * Sets the credentials provider type and the credentials to hadoop based on the provided configuration
     *
     * @param jobConf hadoop job config
     * @param configuration external details configuration
     * @throws CompilationException CompilationException
     */
    private static void setHadoopCredentials(JobConf jobConf, Map<String, String> configuration)
            throws CompilationException {
        AuthenticationType authenticationType = getAuthenticationType(configuration);
        switch (authenticationType) {
            case ANONYMOUS:
                jobConf.set(HADOOP_AUTH_TYPE, HADOOP_AUTH_UNAUTHENTICATED);
                break;
            case IMPERSONATE_SERVICE_ACCOUNT:
                String impersonateServiceAccount = configuration.get(IMPERSONATE_SERVICE_ACCOUNT_FIELD_NAME);
                jobConf.set(IMPERSONATE_SERVICE_ACCOUNT, impersonateServiceAccount);
                setJsonCredentials(jobConf, configuration);
                break;
            case SERVICE_ACCOUNT_KEY_JSON_CREDENTIALS:
                setJsonCredentials(jobConf, configuration);
                break;
            case BAD_AUTHENTICATION:
                throw new CompilationException(ErrorCode.NO_VALID_AUTHENTICATION_PARAMS_PROVIDED);
        }
    }

    /**
     * Sets the Json credentials to hadoop job configuration
     * Note:
     * Setting these values instead of HADOOP_AUTH_SERVICE_ACCOUNT_JSON_KEY_FILE_PATH is supported
     * in com.google.cloud.bigdataoss:util-hadoop only up to version hadoop3-2.2.x and is removed in
     * version 3.x.y, which also removed support for hadoop-2
     *
     * @param jobConf hadoop job config
     * @param configuration external details configuration
     * @throws CompilationException CompilationException
     */
    private static void setJsonCredentials(JobConf jobConf, Map<String, String> configuration)
            throws CompilationException {
        try {
            String jsonCredentials = configuration.get(JSON_CREDENTIALS_FIELD_NAME);
            JsonNode jsonCreds = JSON_CREDENTIALS_OBJECT_MAPPER.readTree(jsonCredentials);
            jobConf.set(GCSConstants.HadoopAuthServiceAccount.PRIVATE_KEY_ID,
                    jsonCreds.get(GCSConstants.JsonCredentials.PRIVATE_KEY_ID).asText());
            jobConf.set(GCSConstants.HadoopAuthServiceAccount.PRIVATE_KEY,
                    jsonCreds.get(GCSConstants.JsonCredentials.PRIVATE_KEY).asText());
            jobConf.set(GCSConstants.HadoopAuthServiceAccount.CLIENT_EMAIL,
                    jsonCreds.get(GCSConstants.JsonCredentials.CLIENT_EMAIL).asText());
        } catch (JsonProcessingException e) {
            throw CompilationException.create(EXTERNAL_SOURCE_ERROR, e, "Unable to parse Json Credentials",
                    getMessageOrToString(e));
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
}
