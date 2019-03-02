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
package org.apache.hyracks.ipc.security;

import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.SecureRandom;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManagerFactory;

import org.apache.hyracks.api.network.INetworkSecurityConfig;
import org.apache.hyracks.api.network.INetworkSecurityManager;
import org.apache.hyracks.api.network.ISocketChannelFactory;
import org.apache.hyracks.ipc.sockets.PlainSocketChannelFactory;
import org.apache.hyracks.ipc.sockets.SslSocketChannelFactory;

public class NetworkSecurityManager implements INetworkSecurityManager {

    private volatile INetworkSecurityConfig config;
    private final ISocketChannelFactory sslSocketFactory;
    private static final String TSL_VERSION = "TLSv1.2";

    public NetworkSecurityManager(INetworkSecurityConfig config) {
        this.config = config;
        if (config.isSslEnabled()) {
            System.setProperty("javax.net.ssl.trustStore", config.getTrustStoreFile().getAbsolutePath());
            System.setProperty("javax.net.ssl.trustStorePassword", config.getKeyStorePassword());
        }
        sslSocketFactory = new SslSocketChannelFactory(this);
    }

    @Override
    public SSLContext newSSLContext() {
        try {
            final char[] password = getKeyStorePassword();
            KeyStore engineKeyStore = config.getKeyStore();
            if (engineKeyStore == null) {
                engineKeyStore = loadKeyStoreFromFile(password);
            }
            final String defaultAlgorithm = KeyManagerFactory.getDefaultAlgorithm();
            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(defaultAlgorithm);
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(defaultAlgorithm);
            keyManagerFactory.init(engineKeyStore, password);
            final KeyStore trustStore = loadTrustStoreFromFile(password);
            trustManagerFactory.init(trustStore);
            SSLContext ctx = SSLContext.getInstance(TSL_VERSION);
            ctx.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), new SecureRandom());
            return ctx;
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to create SSLEngine", ex);
        }
    }

    @Override
    public SSLEngine newSSLEngine() {
        try {
            SSLContext ctx = newSSLContext();
            return ctx.createSSLEngine();
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to create SSLEngine", ex);
        }
    }

    public ISocketChannelFactory getSocketChannelFactory() {
        if (config.isSslEnabled()) {
            return sslSocketFactory;
        }
        return PlainSocketChannelFactory.INSTANCE;
    }

    @Override
    public INetworkSecurityConfig getConfiguration() {
        return config;
    }

    @Override
    public void setConfiguration(INetworkSecurityConfig config) {
        this.config = config;
    }

    private KeyStore loadKeyStoreFromFile(char[] password) {
        try {
            final KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
            ks.load(new FileInputStream(config.getKeyStoreFile()), password);
            return ks;
        } catch (Exception e) {
            throw new IllegalStateException("failed to load key store", e);
        }
    }

    private KeyStore loadTrustStoreFromFile(char[] password) {
        try {
            final KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
            ks.load(new FileInputStream(config.getTrustStoreFile()), password);
            return ks;
        } catch (Exception e) {
            throw new IllegalStateException("failed to load trust store", e);
        }
    }

    private char[] getKeyStorePassword() {
        final String pass = config.getKeyStorePassword();
        return pass == null || pass.isEmpty() ? null : pass.toCharArray();
    }
}
