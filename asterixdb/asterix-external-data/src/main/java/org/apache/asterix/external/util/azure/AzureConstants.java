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

import com.azure.core.http.policy.HttpLogDetailLevel;
import com.azure.core.http.policy.HttpLogOptions;

/*
 * Note: Azure Blob and Azure Datalake use identical authentication, so they are using the same properties.
 * If they end up diverging, then properties for AzureBlob and AzureDataLake need to be created.
 */
public class AzureConstants {
    private AzureConstants() {
        throw new AssertionError("do not instantiate");
    }

    // Key max length
    public static final int MAX_KEY_LENGTH_IN_BYTES = 1024;

    public static final HttpLogOptions HTTP_LOG_OPTIONS = new HttpLogOptions();
    static {
        HTTP_LOG_OPTIONS.setLogLevel(HttpLogDetailLevel.BASIC);
        HTTP_LOG_OPTIONS.addAllowedQueryParamName("restype");
        HTTP_LOG_OPTIONS.addAllowedQueryParamName("comp");
        HTTP_LOG_OPTIONS.addAllowedQueryParamName("prefix");
    }
    /*
     * Asterix Configuration Keys
     */
    public static final String MANAGED_IDENTITY_FIELD_NAME = "managedIdentity";
    public static final String ACCOUNT_NAME_FIELD_NAME = "accountName";
    public static final String ACCOUNT_KEY_FIELD_NAME = "accountKey";
    public static final String SHARED_ACCESS_SIGNATURE_FIELD_NAME = "sharedAccessSignature";
    public static final String TENANT_ID_FIELD_NAME = "tenantId";
    public static final String CLIENT_ID_FIELD_NAME = "clientId";
    public static final String CLIENT_SECRET_FIELD_NAME = "clientSecret";
    public static final String ENDPOINT_FIELD_NAME = "endpoint";

    /*
     * Hadoop-Azure
     */
    public static final String HADOOP_AZURE_PROTOCOL = "abfss";

    /*
     * Hadoop-Azure
     */
    // placeholders that are replaceable
    public static final String HADOOP_TENANT_ID_PLACEHOLDER = "{TENANT_ID}";

    // auth type key
    public static final String HADOOP_AUTH_TYPE = "fs.azure.account.auth.type";

    // shared key auth
    public static final String HADOOP_SHARED_KEY_AUTH_TYPE = "SharedKey";
    public static final String HADOOP_FS_ACCOUNT_KEY_PREFIX = "fs.azure.account.key";

    // shared access signature auth
    public static final String HADOOP_SAS_AUTH_TYPE = "SAS";
    public static final String HADOOP_FS_FIXED_SAS_PREFIX = "fs.azure.sas.fixed.token";

    // OAuth
    public static final String HADOOP_OAUTH_TYPE_OAUTH = "OAuth";
    public static final String HADOOP_OAUTH_PROVIDER_TYPE = "fs.azure.account.oauth.provider.type";
    public static final String HADOOP_CLIENT_ID = "fs.azure.account.oauth2.client.id";
    public static final String HADOOP_OAUTH_ENDPOINT = "fs.azure.account.oauth2.client.endpoint";
    public static final String HADOOP_OAUTH_ENDPOINT_VALUE =
            "https://login.microsoftonline.com/" + HADOOP_TENANT_ID_PLACEHOLDER + "/oauth2/token";

    // client secret auth
    public static final String HADOOP_CLIENT_CREDENTIALS_AUTH_TYPE =
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider";
    public static final String HADOOP_CLIENT_SECRET = "fs.azure.account.oauth2.client.secret";

    // managed identity auth
    public static final String HADOOP_MANAGED_IDENTITY_AUTH_TYPE =
            "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider";
    public static final String HADOOP_MANAGED_IDENTITY_ENDPOINT = "fs.azure.account.oauth2.msi.endpoint";
    public static final String HADOOP_MANAGED_IDENTITY_ENDPOINT_VALUE =
            "http://169.254.169.254/metadata/identity/oauth2/token";
}