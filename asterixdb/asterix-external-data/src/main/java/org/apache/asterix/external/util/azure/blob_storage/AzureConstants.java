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

/*
 * Note: Azure Blob and Azure Datalake use identical authentication, so they are using the same properties.
 * If they end up diverging, then properties for AzureBlob and AzureDataLake need to be created.
 */
public class AzureConstants {
    private AzureConstants() {
        throw new AssertionError("do not instantiate");
    }

    /*
     * Asterix Configuration Keys
     */
    public static final String MANAGED_IDENTITY_ID_FIELD_NAME = "managedIdentityId";
    public static final String ACCOUNT_NAME_FIELD_NAME = "accountName";
    public static final String ACCOUNT_KEY_FIELD_NAME = "accountKey";
    public static final String SHARED_ACCESS_SIGNATURE_FIELD_NAME = "sharedAccessSignature";
    public static final String TENANT_ID_FIELD_NAME = "tenantId";
    public static final String CLIENT_ID_FIELD_NAME = "clientId";
    public static final String CLIENT_SECRET_FIELD_NAME = "clientSecret";
    public static final String CLIENT_CERTIFICATE_FIELD_NAME = "clientCertificate";
    public static final String CLIENT_CERTIFICATE_PASSWORD_FIELD_NAME = "clientCertificatePassword";
    public static final String ENDPOINT_FIELD_NAME = "endpoint";

    // Specific Azure data lake property
    /*
    The behavior of Data Lake (true file system) is to read the files of the specified prefix only, example:
    storage/myData/personal/file1.json
    storage/myData/personal/file2.json
    storage/myData/file3.json
    If the prefix used is "myData", then only the file file3.json is read. However, if the property "recursive"
    is set to "true" when creating the external dataset, then it goes recursively overall the paths, and the result
    is file1.json, file2.json and file3.json.
     */
    public static final String RECURSIVE_FIELD_NAME = "recursive";

    /*
     * Hadoop-Azure
     */
    //Used when accountName and accessKey are provided
    public static final String HADOOP_AZURE_FS_ACCOUNT_KEY = "fs.azure.account.key";
    //Used when a connectionString is provided
    public static final String HADOOP_AZURE_FS_SAS = "fs.azure.sas";
    public static final String HADOOP_AZURE_BLOB_PROTOCOL = "wasbs";
    public static final String HADOOP_AZURE_DATALAKE_PROTOCOL = "abfss";
}
