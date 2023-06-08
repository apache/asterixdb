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
    // AWS S3 constants and placeholders
    public static final String S3_ACCESS_KEY_ID_PLACEHOLDER = "%accessKeyId%";
    public static final String S3_ACCESS_KEY_ID_DEFAULT = "dummyAccessKey";
    public static final String S3_SECRET_ACCESS_KEY_PLACEHOLDER = "%secretAccessKey%";
    public static final String S3_SECRET_ACCESS_KEY_DEFAULT = "dummySecretKey";
    public static final String S3_REGION_PLACEHOLDER = "%region%";
    public static final String S3_REGION_DEFAULT = "us-west-2";
    public static final String S3_SERVICE_ENDPOINT_PLACEHOLDER = "%serviceEndpoint%";
    public static final String S3_SERVICE_ENDPOINT_DEFAULT = "http://127.0.0.1:8001";
    public static final String S3_TEMPLATE = "(\"accessKeyId\"=\"" + S3_ACCESS_KEY_ID_DEFAULT + "\"),\n"
            + "(\"secretAccessKey\"=\"" + S3_SECRET_ACCESS_KEY_DEFAULT + "\"),\n" + "(\"region\"=\""
            + S3_REGION_PLACEHOLDER + "\"),\n" + "(\"serviceEndpoint\"=\"" + S3_SERVICE_ENDPOINT_PLACEHOLDER + "\")";
    public static final String S3_TEMPLATE_DEFAULT = "(\"accessKeyId\"=\"" + S3_ACCESS_KEY_ID_DEFAULT + "\"),\n"
            + "(\"secretAccessKey\"=\"" + S3_SECRET_ACCESS_KEY_DEFAULT + "\"),\n" + "(\"region\"=\"" + S3_REGION_DEFAULT
            + "\"),\n" + "(\"serviceEndpoint\"=\"" + S3_SERVICE_ENDPOINT_DEFAULT + "\")";

    // Azure blob storage constants and placeholders
    public static class Azure {
        // account name
        public static final String ACCOUNT_NAME_PLACEHOLDER = "%azure-accountname%";
        public static final String AZURITE_ACCOUNT_NAME_DEFAULT = "devstoreaccount1";
        public static final int AZURITE_PORT = 15055;
        public static final String AZURITE_HOSTNAME = "127.0.0.1:" + AZURITE_PORT;
        public static final String AZURITE_ENDPOINT =
                "http://127.0.0.1:" + AZURITE_PORT + "/" + AZURITE_ACCOUNT_NAME_DEFAULT;

        // account key
        public static final String ACCOUNT_KEY_PLACEHOLDER = "%azure-accountkey%";
        public static final String AZURITE_ACCOUNT_KEY_DEFAULT =
                "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

        // SAS token: this is generated and assigned at runtime at the start of the test
        public static final String SAS_TOKEN_PLACEHOLDER = "%azure-sas%";
        public static String sasToken = "";

        // blob endpoint
        public static final String BLOB_ENDPOINT_PLACEHOLDER = "%azure-endpoint%";
        public static final String BLOB_ENDPOINT_DEFAULT = AZURITE_ENDPOINT;

        public static final String MANAGED_IDENTITY_ID_PLACEHOLDER = "%azure-managedidentityid%";
        public static final String MANAGED_IDENTITY_ID_DEFAULT = "myManagedIdentityId";

        public static final String CLIENT_ID_PLACEHOLDER = "%azure-clientid%";
        public static final String CLIENT_ID_DEFAULT = "myClientId";

        public static final String CLIENT_SECRET_PLACEHOLDER = "%azure-clientsecret%";
        public static final String CLIENT_SECRET_DEFAULT = "myClientSecret";

        public static final String CLIENT_CERTIFICATE_PLACEHOLDER = "%azure-clientcertificate%";
        public static final String CLIENT_CERTIFICATE_DEFAULT = "myClientCertificate";

        public static final String CLIENT_CERTIFICATE_PASSWORD_PLACEHOLDER = "%azure-clientcertificatepassword%";
        public static final String CLIENT_CERTIFICATE_PASSWORD_DEFAULT = "myClientCertificatePassword";

        public static final String TENANT_ID_PLACEHOLDER = "%azure-tenantid%";
        public static final String TENANT_ID_DEFAULT = "myTenantId";

        // azure template and default template
        public static final String TEMPLATE = "(\"accountName\"=\"" + AZURITE_ACCOUNT_NAME_DEFAULT + "\"),\n"
                + "(\"accountKey\"=\"" + AZURITE_ACCOUNT_KEY_DEFAULT + "\"),\n" + "(\"endpoint\"=\""
                + BLOB_ENDPOINT_PLACEHOLDER + "\")";
        public static final String TEMPLATE_DEFAULT = TEMPLATE;
    }
}
