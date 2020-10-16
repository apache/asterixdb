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
package org.apache.asterix.test.common;

public class TestConstants {
    // AWS S3 constants and place holders
    public static final String S3_ACCESS_KEY_ID_PLACEHOLDER = "%accessKeyId%";
    public static final String S3_ACCESS_KEY_ID_DEFAULT = "dummyAccessKey";
    public static final String S3_SECRET_ACCESS_KEY_PLACEHOLDER = "%secretAccessKey%";
    public static final String S3_SECRET_ACCESS_KEY_DEFAULT = "dummySecretKey";
    public static final String S3_REGION_PLACEHOLDER = "%region%";
    public static final String S3_REGION_DEFAULT = "us-west-2";
    public static final String S3_SERVICE_ENDPOINT_PLACEHOLDER = "%serviceEndpoint%";
    public static final String S3_SERVICE_ENDPOINT_DEFAULT = "http://localhost:8001";
    public static final String S3_TEMPLATE = "(\"accessKeyId\"=\"" + S3_ACCESS_KEY_ID_DEFAULT + "\"),\n"
            + "(\"secretAccessKey\"=\"" + S3_SECRET_ACCESS_KEY_DEFAULT + "\"),\n" + "(\"region\"=\""
            + S3_REGION_PLACEHOLDER + "\"),\n" + "(\"serviceEndpoint\"=\"" + S3_SERVICE_ENDPOINT_PLACEHOLDER + "\")";
    public static final String S3_TEMPLATE_DEFAULT = "(\"accessKeyId\"=\"" + S3_ACCESS_KEY_ID_DEFAULT + "\"),\n"
            + "(\"secretAccessKey\"=\"" + S3_SECRET_ACCESS_KEY_DEFAULT + "\"),\n" + "(\"region\"=\"" + S3_REGION_DEFAULT
            + "\"),\n" + "(\"serviceEndpoint\"=\"" + S3_SERVICE_ENDPOINT_DEFAULT + "\")";

    // Azure blob storage constants and place holders
    public static final String AZURE_ACCOUNT_NAME_PLACEHOLDER = "%accountName%";
    public static final String AZURE_AZURITE_ACCOUNT_NAME_DEFAULT = "devstoreaccount1";
    public static final String AZURE_ACCOUNT_KEY_PLACEHOLDER = "%accountKey%";
    public static final String AZURE_AZURITE_ACCOUNT_KEY_DEFAULT =
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsu" + "Fq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
    public static final String AZURE_BLOB_ENDPOINT_PLACEHOLDER = "%blobEndpoint%";
    public static final String AZURE_BLOB_ENDPOINT_DEFAULT =
            "http://localhost:20000/" + AZURE_AZURITE_ACCOUNT_NAME_DEFAULT;
    public static final String AZURE_TEMPLATE = "(\"accountName\"=\"" + AZURE_AZURITE_ACCOUNT_NAME_DEFAULT + "\"),\n"
            + "(\"accountKey\"=\"" + AZURE_AZURITE_ACCOUNT_KEY_DEFAULT + "\"),\n" + "(\"blobEndpoint\"=\""
            + AZURE_BLOB_ENDPOINT_PLACEHOLDER + "\")";
    public static final String AZURE_TEMPLATE_DEFAULT = "(\"accountName\"=\"" + AZURE_AZURITE_ACCOUNT_NAME_DEFAULT
            + "\"),\n" + "(\"accountKey\"=\"" + AZURE_AZURITE_ACCOUNT_KEY_DEFAULT + "\"),\n" + "(\"blobEndpoint\"=\""
            + AZURE_BLOB_ENDPOINT_DEFAULT + "\")";
}
