/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.asterix.common.functions;

/**
 * Names stored in the metadata of an external HTTP function. Each {@code OPTION_*} is both a field of the
 * function's WITH clause and the key its value is stored under in the function's resources.
 */
// WARNING: These values are stored in function metadata. Do not rename.
public final class HttpFunctionConstants {

    /** First element of the external identifier of a function that names its URL inline. */
    public static final String TARGET_URL = "url";
    /** First element of the external identifier of a function that names a link. */
    public static final String TARGET_LINK = "link";

    public static final String OPTION_METHOD = "method";
    public static final String OPTION_BODY = "body";
    public static final String OPTION_PATH = "path";
    public static final String OPTION_HEADERS = "headers";
    public static final String OPTION_TIMEOUT = "timeout";
    /**
     * Whether the arguments sent as {@code name=value} pairs, in a GET query string or a URLENCODED body, are
     * percent-encoded: if true (the default) by the runtime, if false by the caller, whose values are sent as given,
     * like curl's {@code --data-urlencode} and {@code --data}. Refused for any other body; a path reference is
     * always encoded.
     */
    public static final String OPTION_URLENCODE = "urlencode";
    public static final String OPTION_ON_ERROR = "on-error";

    public static final String OPTION_USERNAME = "username";
    public static final String OPTION_PASSWORD = "password";
    public static final String OPTION_BEARER_TOKEN = "bearerToken";
    public static final String OPTION_OAUTH_TOKEN_URI = "oauthTokenUri";
    public static final String OPTION_OAUTH_CLIENT_ID = "oauthClientId";
    public static final String OPTION_OAUTH_CLIENT_SECRET = "oauthClientSecret";
    public static final String OPTION_OAUTH_ALLOWED_SCOPES = "oauthAllowedScopes";
    public static final String OPTION_CERTIFICATE = "certificate";

    private HttpFunctionConstants() {
    }
}
