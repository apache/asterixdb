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
package org.apache.asterix.external.util.http;

public class HttpConstants {

    private HttpConstants() {
        throw new AssertionError("do not instantiate");
    }

    public static final String USERNAME_FIELD_NAME = "username";
    public static final String PASSWORD_FIELD_NAME = "password";
    public static final String BEARER_TOKEN_FIELD_NAME = "bearerToken";
    public static final String OAUTH_TOKEN_URI_FIELD_NAME = "oauthTokenUri";
    public static final String OAUTH_CLIENT_ID_FIELD_NAME = "oauthClientId";
    public static final String OAUTH_CLIENT_SECRET_FIELD_NAME = "oauthClientSecret";
    public static final String OAUTH_ALLOWED_SCOPES_FIELD_NAME = "oauthAllowedScopes";
    public static final String ENDPOINT_FIELD_NAME = "endpoint";
}
