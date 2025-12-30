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
package org.apache.asterix.external.util.iceberg.nessie;

public class NessieConstants {

    private NessieConstants() {
        throw new AssertionError("do not instantiate");
    }

    public static final String NESSIE_AUTHENTICATION_TYPE_FIELD_NAME = "nessie.authentication.type";

    public static final String NESSIE_AUTHENTICATION_TYPE_OAUTH2 = "OAUTH2";
    public static final String NESSIE_AUTHENTICATION_OAUTH2_ISSUER_URL_FIELD_NAME =
            "nessie.authentication.oauth2.issuer-url";
    public static final String NESSIE_AUTHENTICATION_OAUTH2_GRANT_TYPE_FIELD_NAME =
            "nessie.authentication.oauth2.grant-type";
    public static final String NESSIE_AUTHENTICATION_OAUTH2_GRANT_CREDENTIALS = "client_credentials";
    public static final String NESSIE_AUTHENTICATION_OAUTH2_CLIENT_ID_FIELD_NAME =
            "nessie.authentication.oauth2.client-id";
    public static final String NESSIE_AUTHENTICATION_OAUTH2_CLIENT_SECRET_FIELD_NAME =
            "nessie.authentication.oauth2.client-secret";
    public static final String NESSIE_AUTHENTICATION_OAUTH2_ALLOWED_SCOPES =
            "nessie.authentication.oauth2.client-scopes";

    public static final String NESSIE_AUTHENTICATION_TYPE_BEARER = "BEARER";
    public static final String NESSIE_AUTHENTICATION_BEARER_TOKEN_FIELD_NAME = "nessie.authentication.token";

    public static final String NESSIE_AUTHENTICATION_TYPE_BASIC = "BASIC";
    public static final String NESSIE_AUTHENTICATION_USERNAME_FIELD_NAME = "nessie.authentication.username";
    public static final String NESSIE_AUTHENTICATION_PASSWORD_FIELD_NAME = "nessie.authentication.password";
}
