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
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Optional;

import org.apache.hyracks.api.network.INetworkSecurityConfig;

public class NetworkSecurityConfig implements INetworkSecurityConfig {

    private static final long serialVersionUID = 2L;
    private static final char[] INTEGRITY_PASSWORD = NetworkSecurityConfig.class.getName().toCharArray();
    private final boolean sslEnabled;
    private final File keyStoreFile;
    private final File trustStoreFile;
    private final String keyStorePassword;
    private transient KeyStore keyStore;
    private transient KeyStore trustStore;

    private NetworkSecurityConfig(boolean sslEnabled, String keyStoreFile, String keyStorePassword,
            String trustStoreFile, KeyStore keyStore, KeyStore trustStore) {
        this.sslEnabled = sslEnabled;
        this.keyStoreFile = keyStoreFile != null ? new File(keyStoreFile) : null;
        this.keyStorePassword = keyStorePassword;
        this.trustStoreFile = trustStoreFile != null ? new File(trustStoreFile) : null;
        this.keyStore = keyStore;
        this.trustStore = trustStore;
    }

    public static NetworkSecurityConfig of(boolean sslEnabled, String keyStoreFile, String keyStorePassword,
            String trustStoreFile) {
        return new NetworkSecurityConfig(sslEnabled, keyStoreFile, keyStorePassword, trustStoreFile, null, null);
    }

    public static NetworkSecurityConfig of(boolean sslEnabled, KeyStore keyStore, String keyStorePassword,
            KeyStore trustStore) {
        return new NetworkSecurityConfig(sslEnabled, null, keyStorePassword, null, keyStore, trustStore);
    }

    @Override
    public boolean isSslEnabled() {
        return sslEnabled;
    }

    @Override
    public File getKeyStoreFile() {
        return keyStoreFile;
    }

    @Override
    public String getKeyStorePassword() {
        return keyStorePassword;
    }

    @Override
    public KeyStore getKeyStore() {
        return keyStore;
    }

    @Override
    public KeyStore getTrustStore() {
        return trustStore;
    }

    @Override
    public File getTrustStoreFile() {
        return trustStoreFile;
    }

    @Override
    public Optional<InetAddress> getRMIBindAddress() {
        return Optional.empty();
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        writeStore(keyStore, out);
        writeStore(trustStore, out);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        keyStore = readStore(in);
        trustStore = readStore(in);
    }

    private void writeStore(KeyStore keyStore, ObjectOutputStream out) throws IOException {
        if (keyStore == null) {
            out.writeUTF("");
            return;
        }
        out.writeUTF(keyStore.getType());
        try {
            keyStore.store(out, INTEGRITY_PASSWORD);
        } catch (KeyStoreException | NoSuchAlgorithmException | CertificateException e) {
            throw new IllegalStateException(e);
        }
    }

    private KeyStore readStore(ObjectInputStream in) throws IOException {
        String keyStoreType = in.readUTF();
        if (keyStoreType.isEmpty()) {
            return null;
        }
        try {
            KeyStore store = KeyStore.getInstance(keyStoreType);
            store.load(in, INTEGRITY_PASSWORD);
            return store;
        } catch (KeyStoreException | NoSuchAlgorithmException | CertificateException e) {
            throw new IllegalStateException(e);
        }
    }
}
