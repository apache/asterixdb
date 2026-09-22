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
import java.io.Serializable;
import java.net.InetAddress;
import java.security.KeyStore;
import java.util.Optional;

import io.netty.handler.ssl.ClientAuth;

public interface INetworkSecurityConfig extends Serializable {

    /**
     * Indicates if SSL is enabled
     *
     * @return true if ssl is enabled. Otherwise false.
     */
    boolean isSslEnabled();

    /**
     * Indicates if any authentication being performed should mutual (e.g. mTLS, SCRAM-SHA)
     *
     * @return true if mutual auth should be used. Otherwise false.
     */
    boolean useMutualAuth();

    /**
     * Indicates if the certificate a peer presents on a secured cluster connection must identify the address that
     * peer was reached at. When this is false the peer's chain is still validated against the trust store, but any
     * host holding a certificate it validates is accepted in place of the node that was dialled.
     *
     * @return true if the peer's identity should be verified. Otherwise false.
     */
    default boolean verifyPeerIdentity() {
        return true;
    }

    /**
     * As {@link #verifyPeerIdentity()}, for RMI connections. These are separate because RMI reaches a node by the
     * host recorded in the exported stub ({@code java.rmi.server.hostname}), which is configured independently of
     * the address the cluster's own connections use and so may be covered by a different name, or none.
     *
     * @return true if the peer's identity should be verified on RMI connections. Otherwise false.
     */
    default boolean verifyRmiPeerIdentity() {
        return true;
    }

    /**
     * Indicates how to handle client authentication when ssl is enabled
     */
    ClientAuth getClientAuth();

    /**
     * Gets the key store to be used for secured connections
     *
     * @return the key store to be used, if present
     */
    Optional<KeyStore> getKeyStore();

    /**
     * Gets a key store file, password pair to be used if {@link INetworkSecurityConfig#getKeyStore()} returns empty.
     *
     * @return the key store file
     */
    File getKeyStoreFile();

    /**
     * Gets a password to be used to unlock or check integrity of the key store.
     *
     * @return the key store password, or {@link Optional#empty()}
     */
    Optional<char[]> getKeyStorePassword();

    /**
     * Gets the client key store to be used for client auth, if applicable.
     *
     * @return the client key store to be used for client auth, or {@link Optional#empty()}
     */
    Optional<KeyStore> getClientKeyStore();

    /**
     * Gets a client key store file to be used if {@link INetworkSecurityConfig#getClientKeyStore()} returns empty.
     *
     * @return the key store file
     */
    File getClientKeyStoreFile();

    /**
     * Gets a password to be used to unlock or check integrity of the client key store.
     *
     * @return the client key store password, or {@link Optional#empty()}
     */
    Optional<char[]> getClientKeyStorePassword();

    /**
     * Gets the trust store to be used for validating certificates of secured connections
     *
     * @return the trust store to be used
     */
    KeyStore getTrustStore();

    /**
     * Gets a trust store file to be used if {@link INetworkSecurityConfig#getTrustStore()} returns null.
     *
     * @return the trust store file
     */
    File getTrustStoreFile();

    /**
     * The optional address to bind for RMI server sockets; or absent to bind to all addresses / interfaces.
     *
     * @return the optional bind address
     */
    Optional<InetAddress> getRMIBindAddress();
}
