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
package org.apache.asterix.external.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Collection;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

public class TlsUtils {

    private TlsUtils() {
    }

    /**
     * Builds trust managers that trust the certificates of a PEM string and, when includeDefaultCAs is set, the
     * JVM's default certificate authorities too.
     */
    public static TrustManager[] buildTrustManagers(String pemCertificates, boolean includeDefaultCAs)
            throws GeneralSecurityException, IOException {
        Collection<? extends Certificate> certificates = CertificateFactory.getInstance("X.509")
                .generateCertificates(new ByteArrayInputStream(pemCertificates.getBytes(StandardCharsets.UTF_8)));
        if (certificates.isEmpty()) {
            throw new CertificateException("No certificates found in the supplied PEM data");
        }

        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null, null);

        int alias = 0;
        if (includeDefaultCAs) {
            TrustManagerFactory defaultTrustManagerFactory =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            defaultTrustManagerFactory.init((KeyStore) null);
            for (TrustManager trustManager : defaultTrustManagerFactory.getTrustManagers()) {
                if (trustManager instanceof X509TrustManager x509TrustManager) {
                    for (X509Certificate certificate : x509TrustManager.getAcceptedIssuers()) {
                        keyStore.setCertificateEntry("default-ca-" + alias++, certificate);
                    }
                }
            }
        }
        for (Certificate certificate : certificates) {
            keyStore.setCertificateEntry("custom-ca-" + alias++, certificate);
        }

        TrustManagerFactory trustManagerFactory =
                TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(keyStore);
        return trustManagerFactory.getTrustManagers();
    }
}
