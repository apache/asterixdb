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

import static org.apache.asterix.common.exceptions.ErrorCode.PARAMETERS_REQUIRED;
import static org.apache.asterix.common.exceptions.ErrorCode.PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT;
import static org.apache.asterix.common.exceptions.ErrorCode.REQUIRED_PARAM_IF_PARAM_IS_PRESENT;
import static org.apache.asterix.external.util.http.HttpConstants.BEARER_TOKEN_FIELD_NAME;
import static org.apache.asterix.external.util.http.HttpConstants.OAUTH_ALLOWED_SCOPES_FIELD_NAME;
import static org.apache.asterix.external.util.http.HttpConstants.OAUTH_CLIENT_ID_FIELD_NAME;
import static org.apache.asterix.external.util.http.HttpConstants.OAUTH_CLIENT_SECRET_FIELD_NAME;
import static org.apache.asterix.external.util.http.HttpConstants.OAUTH_TOKEN_URI_FIELD_NAME;
import static org.apache.asterix.external.util.http.HttpConstants.PASSWORD_FIELD_NAME;
import static org.apache.asterix.external.util.http.HttpConstants.USERNAME_FIELD_NAME;
import static org.apache.asterix.external.util.iceberg.IcebergConstants.ICEBERG_URI_PROPERTY_KEY;
import static org.apache.asterix.external.util.iceberg.IcebergConstants.ICEBERG_WAREHOUSE_PROPERTY_KEY;
import static org.apache.asterix.external.util.iceberg.IcebergUtils.validatePropertyExists;
import static org.apache.asterix.external.util.iceberg.nessie.NessieConstants.NESSIE_AUTHENTICATION_BEARER_TOKEN_FIELD_NAME;
import static org.apache.asterix.external.util.iceberg.nessie.NessieConstants.NESSIE_AUTHENTICATION_OAUTH2_ALLOWED_SCOPES;
import static org.apache.asterix.external.util.iceberg.nessie.NessieConstants.NESSIE_AUTHENTICATION_OAUTH2_CLIENT_ID_FIELD_NAME;
import static org.apache.asterix.external.util.iceberg.nessie.NessieConstants.NESSIE_AUTHENTICATION_OAUTH2_CLIENT_SECRET_FIELD_NAME;
import static org.apache.asterix.external.util.iceberg.nessie.NessieConstants.NESSIE_AUTHENTICATION_OAUTH2_GRANT_CREDENTIALS;
import static org.apache.asterix.external.util.iceberg.nessie.NessieConstants.NESSIE_AUTHENTICATION_OAUTH2_GRANT_TYPE_FIELD_NAME;
import static org.apache.asterix.external.util.iceberg.nessie.NessieConstants.NESSIE_AUTHENTICATION_OAUTH2_ISSUER_URL_FIELD_NAME;
import static org.apache.asterix.external.util.iceberg.nessie.NessieConstants.NESSIE_AUTHENTICATION_PASSWORD_FIELD_NAME;
import static org.apache.asterix.external.util.iceberg.nessie.NessieConstants.NESSIE_AUTHENTICATION_TYPE_BASIC;
import static org.apache.asterix.external.util.iceberg.nessie.NessieConstants.NESSIE_AUTHENTICATION_TYPE_BEARER;
import static org.apache.asterix.external.util.iceberg.nessie.NessieConstants.NESSIE_AUTHENTICATION_TYPE_FIELD_NAME;
import static org.apache.asterix.external.util.iceberg.nessie.NessieConstants.NESSIE_AUTHENTICATION_TYPE_OAUTH2;
import static org.apache.asterix.external.util.iceberg.nessie.NessieConstants.NESSIE_AUTHENTICATION_USERNAME_FIELD_NAME;

import java.util.Map;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.nessie.NessieCatalog;

public class NessieUtils {
    public enum AuthenticationType {
        ANONYMOUS,
        OAUTH,
        BEARER_TOKEN,
        BASIC,
        BAD_AUTHENTICATION
    }

    private NessieUtils() {
        throw new AssertionError("do not instantiate");
    }

    public static void setNessieCatalogProperties(Map<String, String> catalogProperties) throws CompilationException {
        catalogProperties.put(CatalogProperties.CATALOG_IMPL, NessieCatalog.class.getName());
        catalogProperties.put(CatalogProperties.URI, catalogProperties.get(ICEBERG_URI_PROPERTY_KEY));
        setAuthentication(catalogProperties);
    }

    public static void validateRequiredProperties(Map<String, String> catalogProperties) throws CompilationException {
        validatePropertyExists(catalogProperties, ICEBERG_URI_PROPERTY_KEY, PARAMETERS_REQUIRED);
        validatePropertyExists(catalogProperties, ICEBERG_WAREHOUSE_PROPERTY_KEY, PARAMETERS_REQUIRED);
    }

    public static void validateNessieRestRequiredProperties(Map<String, String> catalogProperties)
            throws CompilationException {
        validatePropertyExists(catalogProperties, ICEBERG_URI_PROPERTY_KEY, PARAMETERS_REQUIRED);
    }

    public static void setAuthentication(Map<String, String> catalogProperties) throws CompilationException {
        AuthenticationType authenticationType = getAuthenticationType(catalogProperties);
        switch (authenticationType) {
            case ANONYMOUS:
                // no-op, no auth to provide
                break;
            case OAUTH:
                setOAuthProperties(catalogProperties);
                break;
            case BEARER_TOKEN:
                setBearerTokenProperties(catalogProperties);
                break;
            case BASIC:
                setBasicProperties(catalogProperties);
                break;
            default:
                throw new IllegalArgumentException("Invalid Nessie authentication configuration");
        }
    }

    public static AuthenticationType getAuthenticationType(Map<String, String> configuration) {
        if (noAuth(configuration)) {
            return AuthenticationType.ANONYMOUS;
        }

        String tokenUri = configuration.get(OAUTH_TOKEN_URI_FIELD_NAME);
        String bearerToken = configuration.get(BEARER_TOKEN_FIELD_NAME);
        String username = configuration.get(USERNAME_FIELD_NAME);

        if (tokenUri != null) {
            return AuthenticationType.OAUTH;
        } else if (bearerToken != null) {
            return AuthenticationType.BEARER_TOKEN;
        } else if (username != null) {
            return AuthenticationType.BASIC;
        } else {
            return AuthenticationType.BAD_AUTHENTICATION;
        }
    }

    private static void setOAuthProperties(Map<String, String> catalogProperties) throws CompilationException {
        String tokenUri = catalogProperties.get(OAUTH_TOKEN_URI_FIELD_NAME);
        String clientId = catalogProperties.get(OAUTH_CLIENT_ID_FIELD_NAME);
        String clientSecret = catalogProperties.get(OAUTH_CLIENT_SECRET_FIELD_NAME);
        String allowedScopes = catalogProperties.get(OAUTH_ALLOWED_SCOPES_FIELD_NAME);

        String notAllowed =
                getNonNull(catalogProperties, BEARER_TOKEN_FIELD_NAME, USERNAME_FIELD_NAME, PASSWORD_FIELD_NAME);
        if (notAllowed != null) {
            throw new CompilationException(PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT, notAllowed,
                    OAUTH_CLIENT_ID_FIELD_NAME);
        }

        if (clientId == null || clientSecret == null) {
            throw CompilationException.create(REQUIRED_PARAM_IF_PARAM_IS_PRESENT,
                    clientId == null ? OAUTH_CLIENT_ID_FIELD_NAME : OAUTH_CLIENT_SECRET_FIELD_NAME,
                    OAUTH_TOKEN_URI_FIELD_NAME);
        }
        catalogProperties.put(NESSIE_AUTHENTICATION_TYPE_FIELD_NAME, NESSIE_AUTHENTICATION_TYPE_OAUTH2);
        catalogProperties.put(NESSIE_AUTHENTICATION_OAUTH2_GRANT_TYPE_FIELD_NAME,
                NESSIE_AUTHENTICATION_OAUTH2_GRANT_CREDENTIALS);
        catalogProperties.put(NESSIE_AUTHENTICATION_OAUTH2_ISSUER_URL_FIELD_NAME, tokenUri);
        catalogProperties.put(NESSIE_AUTHENTICATION_OAUTH2_CLIENT_ID_FIELD_NAME, clientId);
        catalogProperties.put(NESSIE_AUTHENTICATION_OAUTH2_CLIENT_SECRET_FIELD_NAME, clientSecret);
        if (allowedScopes != null) {
            catalogProperties.put(NESSIE_AUTHENTICATION_OAUTH2_ALLOWED_SCOPES, allowedScopes);
        }
    }

    private static void setBearerTokenProperties(Map<String, String> catalogProperties) throws CompilationException {
        String bearerToken = catalogProperties.get(BEARER_TOKEN_FIELD_NAME);
        String notAllowed = getNonNull(catalogProperties, USERNAME_FIELD_NAME, PASSWORD_FIELD_NAME);
        if (notAllowed != null) {
            throw new CompilationException(PARAM_NOT_ALLOWED_IF_PARAM_IS_PRESENT, notAllowed, BEARER_TOKEN_FIELD_NAME);
        }

        catalogProperties.put(NESSIE_AUTHENTICATION_TYPE_FIELD_NAME, NESSIE_AUTHENTICATION_TYPE_BEARER);
        catalogProperties.put(NESSIE_AUTHENTICATION_BEARER_TOKEN_FIELD_NAME, bearerToken);
    }

    private static void setBasicProperties(Map<String, String> catalogProperties) throws CompilationException {
        String username = catalogProperties.get(USERNAME_FIELD_NAME);
        String password = catalogProperties.get(PASSWORD_FIELD_NAME);
        if (password != null) {
            throw CompilationException.create(REQUIRED_PARAM_IF_PARAM_IS_PRESENT, PASSWORD_FIELD_NAME,
                    USERNAME_FIELD_NAME);
        }

        catalogProperties.put(NESSIE_AUTHENTICATION_TYPE_FIELD_NAME, NESSIE_AUTHENTICATION_TYPE_BASIC);
        catalogProperties.put(NESSIE_AUTHENTICATION_USERNAME_FIELD_NAME, username);
        catalogProperties.put(NESSIE_AUTHENTICATION_PASSWORD_FIELD_NAME, password);
    }

    private static boolean noAuth(Map<String, String> configuration) {
        return getNonNull(configuration, USERNAME_FIELD_NAME, PASSWORD_FIELD_NAME, BEARER_TOKEN_FIELD_NAME,
                OAUTH_TOKEN_URI_FIELD_NAME, OAUTH_CLIENT_ID_FIELD_NAME, OAUTH_CLIENT_SECRET_FIELD_NAME,
                OAUTH_ALLOWED_SCOPES_FIELD_NAME) == null;
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
