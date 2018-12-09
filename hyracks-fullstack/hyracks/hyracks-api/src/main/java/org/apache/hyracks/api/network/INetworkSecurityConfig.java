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
package org.apache.hyracks.api.network;

import java.io.File;
import java.security.KeyStore;

public interface INetworkSecurityConfig {

    /**
     * Indicates if SSL is enabled
     *
     * @return true if ssl is enabled. Otherwise false.
     */
    boolean isSslEnabled();

    /**
     * Gets the key store to be used for secured connections
     *
     * @return the key store to be used
     */
    KeyStore getKeyStore();

    /**
     * Gets a key store file to be used if {@link INetworkSecurityConfig#getKeyStore()} returns null.
     *
     * @return the key store file
     */
    File getKeyStoreFile();

    /**
     * Gets the password for the key store file.
     *
     * @return the password to the key store file
     */
    String getKeyStorePassword();

    /**
     * Gets a trust store file to be used for validating certificates of secured connections.
     *
     * @return the trust store file
     */
    File getTrustStoreFile();
}