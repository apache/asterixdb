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
package org.apache.asterix.external.util.azure;

import static org.apache.asterix.external.util.azure.AzureConstants.ACCOUNT_KEY_FIELD_NAME;
import static org.apache.asterix.external.util.azure.AzureConstants.CLIENT_ID_FIELD_NAME;
import static org.apache.asterix.external.util.azure.AzureConstants.CLIENT_SECRET_FIELD_NAME;
import static org.apache.asterix.external.util.azure.AzureConstants.HADOOP_AUTH_TYPE;
import static org.apache.asterix.external.util.azure.AzureConstants.HADOOP_AZURE_PROTOCOL;
import static org.apache.asterix.external.util.azure.AzureConstants.HADOOP_CLIENT_CREDENTIALS_AUTH_TYPE;
import static org.apache.asterix.external.util.azure.AzureConstants.HADOOP_CLIENT_ID;
import static org.apache.asterix.external.util.azure.AzureConstants.HADOOP_CLIENT_SECRET;
import static org.apache.asterix.external.util.azure.AzureConstants.HADOOP_FS_ACCOUNT_KEY_PREFIX;
import static org.apache.asterix.external.util.azure.AzureConstants.HADOOP_FS_FIXED_SAS_PREFIX;
import static org.apache.asterix.external.util.azure.AzureConstants.HADOOP_MANAGED_IDENTITY_AUTH_TYPE;
import static org.apache.asterix.external.util.azure.AzureConstants.HADOOP_MANAGED_IDENTITY_ENDPOINT;
import static org.apache.asterix.external.util.azure.AzureConstants.HADOOP_MANAGED_IDENTITY_ENDPOINT_VALUE;
import static org.apache.asterix.external.util.azure.AzureConstants.HADOOP_OAUTH_ENDPOINT;
import static org.apache.asterix.external.util.azure.AzureConstants.HADOOP_OAUTH_ENDPOINT_VALUE;
import static org.apache.asterix.external.util.azure.AzureConstants.HADOOP_OAUTH_PROVIDER_TYPE;
import static org.apache.asterix.external.util.azure.AzureConstants.HADOOP_OAUTH_TYPE_OAUTH;
import static org.apache.asterix.external.util.azure.AzureConstants.HADOOP_SAS_AUTH_TYPE;
import static org.apache.asterix.external.util.azure.AzureConstants.HADOOP_SHARED_KEY_AUTH_TYPE;
import static org.apache.asterix.external.util.azure.AzureConstants.HADOOP_TENANT_ID_PLACEHOLDER;
import static org.apache.asterix.external.util.azure.AzureConstants.MANAGED_IDENTITY_FIELD_NAME;
import static org.apache.asterix.external.util.azure.AzureConstants.SHARED_ACCESS_SIGNATURE_FIELD_NAME;
import static org.apache.asterix.external.util.azure.AzureConstants.TENANT_ID_FIELD_NAME;

import java.util.Map;

import org.apache.asterix.external.util.HDFSUtils;
import org.apache.hadoop.mapred.JobConf;

public class AzureUtils {
    private AzureUtils() {
        throw new AssertionError("do not instantiate");
    }

    public static void configureAzureHdfsJobConf(JobConf conf, Map<String, String> configuration, String endpoint) {
        String accountKey = configuration.get(ACCOUNT_KEY_FIELD_NAME);
        String sharedAccessSignature = configuration.get(SHARED_ACCESS_SIGNATURE_FIELD_NAME);
        String clientSecret = configuration.get(CLIENT_SECRET_FIELD_NAME);
        String managedIdentity = configuration.get(MANAGED_IDENTITY_FIELD_NAME);

        // disable caching FileSystem
        HDFSUtils.disableHadoopFileSystemCache(conf, HADOOP_AZURE_PROTOCOL);

        if (accountKey != null) {
            setSharedKeyCredential(conf, configuration, endpoint);
        } else if (sharedAccessSignature != null) {
            setSasCredential(conf, configuration);
        } else if (managedIdentity != null) {
            setManagedIdentityCredential(conf, configuration);
        } else if (clientSecret != null) {
            setClientSecretCredentials(conf, configuration);
        }
    }

    private static void setSharedKeyCredential(JobConf conf, Map<String, String> configuration, String endpoint) {
        String authTypeKey = HADOOP_AUTH_TYPE + '.' + endpoint;
        conf.set(authTypeKey, HADOOP_SHARED_KEY_AUTH_TYPE);

        String authKey = HADOOP_FS_ACCOUNT_KEY_PREFIX + "." + endpoint;
        String accountKey = configuration.get(ACCOUNT_KEY_FIELD_NAME);
        conf.set(authKey, accountKey);
    }

    private static void setSasCredential(JobConf conf, Map<String, String> configuration) {
        conf.set(HADOOP_AUTH_TYPE, HADOOP_SAS_AUTH_TYPE);

        String sharedAccessSignature = configuration.get(SHARED_ACCESS_SIGNATURE_FIELD_NAME);
        conf.set(HADOOP_FS_FIXED_SAS_PREFIX, sharedAccessSignature);
    }

    private static void setClientSecretCredentials(JobConf conf, Map<String, String> configuration) {
        conf.set(HADOOP_AUTH_TYPE, HADOOP_OAUTH_TYPE_OAUTH);
        conf.set(HADOOP_OAUTH_PROVIDER_TYPE, HADOOP_CLIENT_CREDENTIALS_AUTH_TYPE);

        String tenantId = configuration.get(TENANT_ID_FIELD_NAME);
        String clientId = configuration.get(CLIENT_ID_FIELD_NAME);
        String clientSecret = configuration.get(CLIENT_SECRET_FIELD_NAME);
        String clientEndpoint = HADOOP_OAUTH_ENDPOINT_VALUE.replace(HADOOP_TENANT_ID_PLACEHOLDER, tenantId);

        conf.set(HADOOP_CLIENT_ID, clientId);
        conf.set(HADOOP_CLIENT_SECRET, clientSecret);
        conf.set(HADOOP_OAUTH_ENDPOINT, clientEndpoint);
    }

    private static void setManagedIdentityCredential(JobConf conf, Map<String, String> configuration) {
        conf.set(HADOOP_AUTH_TYPE, HADOOP_OAUTH_TYPE_OAUTH);
        conf.set(HADOOP_OAUTH_PROVIDER_TYPE, HADOOP_MANAGED_IDENTITY_AUTH_TYPE);
        conf.set(HADOOP_MANAGED_IDENTITY_ENDPOINT, HADOOP_MANAGED_IDENTITY_ENDPOINT_VALUE);

        // if clientId is present, it is user-assigned managed identity, otherwise, it's system managed identity
        String clientId = configuration.get(CLIENT_ID_FIELD_NAME);
        if (clientId != null) {
            conf.set(HADOOP_CLIENT_ID, clientId);
        }
    }

    public static String extractEndPoint(String uri) {
        //The URI is in the form http(s)://<accountName>.blob.core.windows.net
        //We need to Remove the protocol (i.e., http(s)://) from the URI
        return uri.substring(uri.indexOf("//") + "//".length());
    }
}
