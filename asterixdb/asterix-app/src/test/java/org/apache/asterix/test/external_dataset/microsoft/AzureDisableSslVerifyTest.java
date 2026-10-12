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
package org.apache.asterix.test.external_dataset.microsoft;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.FileInputStream;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;

import org.apache.asterix.external.util.azure.blob.BlobUtils;
import org.apache.asterix.external.util.azure.datalake.DatalakeUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;

/**
 * {@code disableSslVerify} lets the Azure Blob and Data Lake clients reach an endpoint whose certificate is not
 * trusted, and without it the handshake is refused.
 */
public class AzureDisableSslVerifyTest {

    private static final char[] PASSWORD = "asterixdb".toCharArray();
    private static HttpsServer server;
    private static String endpoint;

    @BeforeClass
    public static void startServer() throws Exception {
        KeyStore keyStore = KeyStore.getInstance("JKS");
        try (FileInputStream in = new FileInputStream("src/test/resources/security/cc/cc.jks")) {
            keyStore.load(in, PASSWORD);
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, PASSWORD);
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(kmf.getKeyManagers(), null, null);

        server = HttpsServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setHttpsConfigurator(new HttpsConfigurator(sslContext));
        server.createContext("/", exchange -> {
            exchange.sendResponseHeaders(404, -1);
            exchange.close();
        });
        server.start();
        endpoint = "https://127.0.0.1:" + server.getAddress().getPort() + "/devstoreaccount1";
    }

    @AfterClass
    public static void stopServer() {
        server.stop(0);
    }

    private static Map<String, String> config(boolean disableSslVerify) {
        Map<String, String> config = new HashMap<>();
        config.put("endpoint", endpoint);
        config.put("accountName", "devstoreaccount1");
        config.put("accountKey", "a2V5");
        config.put("disableSslVerify", String.valueOf(disableSslVerify));
        return config;
    }

    private static boolean failsHandshake(Runnable request) {
        try {
            request.run();
        } catch (RuntimeException e) {
            for (Throwable t = e; t != null; t = t.getCause()) {
                if (t instanceof SSLHandshakeException) {
                    return true;
                }
            }
        }
        return false;
    }

    @Test
    public void blob() throws Exception {
        BlobServiceClient verifying = BlobUtils.buildClient(null, config(false));
        BlobServiceClient notVerifying = BlobUtils.buildClient(null, config(true));
        assertTrue(failsHandshake(() -> verifying.getBlobContainerClient("playground").exists()));
        assertFalse(failsHandshake(() -> notVerifying.getBlobContainerClient("playground").exists()));
    }

    @Test
    public void datalake() throws Exception {
        DataLakeServiceClient verifying = DatalakeUtils.buildClient(null, config(false));
        DataLakeServiceClient notVerifying = DatalakeUtils.buildClient(null, config(true));
        assertTrue(failsHandshake(() -> verifying.getFileSystemClient("playground").exists()));
        assertFalse(failsHandshake(() -> notVerifying.getFileSystemClient("playground").exists()));
    }
}
