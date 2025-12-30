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
package org.apache.asterix.external.util.google.iceberg.auth;

import static org.apache.iceberg.gcp.auth.GoogleAuthManager.DEFAULT_SCOPES;
import static org.apache.iceberg.gcp.auth.GoogleAuthManager.GCP_SCOPES_PROPERTY;

import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.util.google.GCSUtils;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthSession;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;

/**
 * Copy implementation of {@link <a href="https://github.com/apache/iceberg/blob/apache-iceberg-1.10.0/gcp/src/main/java/org/apache/iceberg/gcp/auth/GoogleAuthManager.java">GoogleAuthManager</a>}
 * that supports different authentication methods
 */
public class InMemoryServiceAccountGoogleAuthManager implements AuthManager {
    private final String name;
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private GoogleCredentials credentials;
    private boolean initialized = false;

    public InMemoryServiceAccountGoogleAuthManager(String managerName) {
        this.name = managerName;
    }

    public String name() {
        return name;
    }

    private void initialize(Map<String, String> properties) throws CompilationException {
        if (initialized) {
            return;
        }

        String scopesString = properties.getOrDefault(GCP_SCOPES_PROPERTY, DEFAULT_SCOPES);
        List<String> scopes = Strings.isNullOrEmpty(scopesString) ? ImmutableList.of()
                : ImmutableList.copyOf(SPLITTER.splitToList(scopesString));
        Credentials authCredentials = GCSUtils.buildCredentials(null, properties, scopes);

        if (!(authCredentials instanceof GoogleCredentials)) {
            throw CompilationException.create(ErrorCode.NO_VALID_CREDENTIALS_PROVIDED_FOR_BIGLAKE_METASTORE_CATALOG);
        }
        this.credentials = (GoogleCredentials) authCredentials;
        this.initialized = true;
    }

    /**
     * Initializes and returns a short-lived session, typically for fetching configuration. This
     * implementation reuses the long-lived catalog session logic.
     */
    @Override
    public AuthSession initSession(RESTClient initClient, Map<String, String> properties) {
        return catalogSession(initClient, properties);
    }

    /**
     * Returns a long-lived session tied to the catalog's lifecycle. This session uses Google
     * Application Default Credentials or a specified service account.
     *
     * @param sharedClient The long-lived RESTClient (not used by this implementation for credential
     *     fetching).
     * @param properties Configuration properties for the auth manager.
     * @return A {@link GoogleAuthSession}.
     * @throws UncheckedIOException if credential loading fails.
     */
    @Override
    public AuthSession catalogSession(RESTClient sharedClient, Map<String, String> properties) {
        try {
            initialize(properties);
        } catch (CompilationException e) {
            throw new RuntimeException(e);
        }
        Preconditions.checkState(credentials != null, getClass() + " not initialized or failed to load credentials");
        return new GoogleAuthSession(credentials);
    }

    /**
     * Returns a session for a specific context. Defaults to the catalog session. For GCP, tokens are
     * typically not context-specific in this manner.
     */
    @Override
    public AuthSession contextualSession(SessionCatalog.SessionContext context, AuthSession parent) {
        return parent;
    }

    /** Returns a session for a specific table or view. Defaults to the catalog session. */
    @Override
    public AuthSession tableSession(TableIdentifier table, Map<String, String> properties, AuthSession parent) {
        return parent;
    }

    /** Closes the manager. This is a no-op for GoogleAuthManager. */
    @Override
    public void close() {
    }
}
