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

import java.io.File;
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
    protected final ISocketChannelFactory sslSocketFactory;
    public static final String TLS_VERSION = "TLSv1.2";

    public NetworkSecurityManager(INetworkSecurityConfig config) {
        this.config = config;
        sslSocketFactory = new SslSocketChannelFactory(this);
    }

    @Override
    public SSLContext newSSLContext(boolean clientMode) {
        return newSSLContext(config, clientMode);
    }

    @Override
    public SSLEngine newSSLEngine(boolean clientMode) {
        try {
            SSLEngine sslEngine = newSSLContext(clientMode).createSSLEngine();
            sslEngine.setUseClientMode(clientMode);
            return sslEngine;
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

    public static SSLContext newSSLContext(INetworkSecurityConfig config, boolean clientMode) {
        try {
            KeyStore engineKeyStore;
            final char[] password;
            if (clientMode) {
                password = config.getClientKeyStorePassword().orElse(null);
                engineKeyStore = config.getClientKeyStore().orElse(null);
                if (engineKeyStore == null) {
                    engineKeyStore = loadKeyStoreFromFile(password, config.getClientKeyStoreFile());
                }
            } else {
                password = config.getKeyStorePassword().orElse(null);
                engineKeyStore = config.getKeyStore().orElse(null);
                if (engineKeyStore == null) {
                    engineKeyStore = loadKeyStoreFromFile(password, config.getKeyStoreFile());
                }
            }
            final String defaultAlgorithm = KeyManagerFactory.getDefaultAlgorithm();
            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(defaultAlgorithm);
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(defaultAlgorithm);
            keyManagerFactory.init(engineKeyStore, password);
            KeyStore trustStore = config.getTrustStore();
            if (trustStore == null) {
                trustStore = loadKeyStoreFromFile(password, config.getTrustStoreFile());
            }
            trustManagerFactory.init(trustStore);
            SSLContext ctx = SSLContext.getInstance(TLS_VERSION);
            ctx.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), new SecureRandom());
            return ctx;
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to create SSLEngine", ex);
        }
    }

    private static KeyStore loadKeyStoreFromFile(char[] password, File keystoreFile) {
        try {
            final KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
            ks.load(new FileInputStream(keystoreFile), password);
            return ks;
        } catch (Exception e) {
            throw new IllegalStateException("failed to load key store", e);
        }
    }

}
