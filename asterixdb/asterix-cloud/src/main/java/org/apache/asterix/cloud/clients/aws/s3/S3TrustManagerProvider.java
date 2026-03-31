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
package org.apache.asterix.cloud.clients.aws.s3;

import static org.apache.hyracks.util.annotations.AiProvenance.Agent.CLAUDE_OPUS_4_6;
import static org.apache.hyracks.util.annotations.AiProvenance.ContributionKind.GENERATED;
import static org.apache.hyracks.util.annotations.AiProvenance.Tool.GITHUB_COPILOT;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.apache.hyracks.util.annotations.AiProvenance;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import software.amazon.awssdk.http.TlsTrustManagersProvider;

/**
 * Provides a {@link TlsTrustManagersProvider} that trusts the default system certificates
 * plus any additional PEM-encoded CA certificates supplied via the blobStorageCertificates setting.
 */
@AiProvenance(agent = CLAUDE_OPUS_4_6, tool = GITHUB_COPILOT, contributionKind = GENERATED)
final class S3TrustManagerProvider {

    private static final Logger LOGGER = LogManager.getLogger();

    private S3TrustManagerProvider() {
        throw new AssertionError("do not instantiate");
    }

    /**
     * Creates a {@link TlsTrustManagersProvider} that merges the given PEM certificate(s)
     * with the JVM default trust store.
     *
     * @param pemCertificates PEM-encoded CA certificate(s)
     * @return a provider that returns trust managers trusting both the system CAs and the custom certificate(s)
     */
    static TlsTrustManagersProvider create(Collection<String> pemCertificates) {
        String pemCertificateData = String.join("\n", pemCertificates);
        return () -> buildTrustManagers(pemCertificateData);
    }

    private static TrustManager[] buildTrustManagers(String pemCertificate) {
        try {
            // Parse the PEM certificate(s)
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            Collection<? extends java.security.cert.Certificate> certs =
                    cf.generateCertificates(new ByteArrayInputStream(pemCertificate.getBytes(StandardCharsets.UTF_8)));
            if (certs.isEmpty()) {
                LOGGER.warn("No certificates found in the provided PEM data; falling back to system defaults");
                return getDefaultTrustManagers();
            }

            // Load the default trust store
            TrustManagerFactory defaultTmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            defaultTmf.init((KeyStore) null);

            // Build a new key store with the default certs + custom certs
            KeyStore customKeyStore = KeyStore.getInstance(KeyStore.getDefaultType());
            customKeyStore.load(null, null);

            // Import default trusted certs
            TrustManager[] defaultTrustManagers = defaultTmf.getTrustManagers();
            for (TrustManager tm : defaultTrustManagers) {
                if (tm instanceof javax.net.ssl.X509TrustManager) {
                    for (X509Certificate cert : ((javax.net.ssl.X509TrustManager) tm).getAcceptedIssuers()) {
                        String alias = cert.getSubjectX500Principal().getName();
                        customKeyStore.setCertificateEntry(alias, cert);
                    }
                }
            }

            // Import custom CA certs
            List<? extends java.security.cert.Certificate> certList = new ArrayList<>(certs);
            for (int i = 0; i < certList.size(); i++) {
                customKeyStore.setCertificateEntry("custom-ca-" + i, certList.get(i));
            }

            TrustManagerFactory customTmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            customTmf.init(customKeyStore);
            LOGGER.info("Custom CA certificate(s) loaded successfully ({} certificate(s))", certs.size());
            return customTmf.getTrustManagers();
        } catch (Exception e) {
            LOGGER.warn("Failed to load custom CA certificate; falling back to system defaults", e);
            return getDefaultTrustManagers();
        }
    }

    private static TrustManager[] getDefaultTrustManagers() {
        try {
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init((KeyStore) null);
            return tmf.getTrustManagers();
        } catch (Exception e) {
            throw new IllegalStateException("Unable to initialize default TrustManagerFactory", e);
        }
    }
}
