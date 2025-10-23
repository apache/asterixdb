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

package org.apache.asterix.cloud.azure;

import java.io.File;
import java.io.FileOutputStream;
import java.math.BigInteger;
import java.net.URI;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.X509Certificate;
import java.util.Date;

import org.apache.asterix.cloud.AbstractLSMTest;
import org.apache.asterix.cloud.clients.ICloudGuardian;
import org.apache.asterix.cloud.clients.azure.blobstorage.AzBlobStorageClientConfig;
import org.apache.asterix.cloud.clients.azure.blobstorage.AzBlobStorageCloudClient;
import org.apache.hyracks.util.StorageUtil;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.testcontainers.azure.AzuriteContainer;
import org.testcontainers.utility.MountableFile;

import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import reactor.netty.http.client.HttpClient;

public class LSMAzBlobStorageTest extends AbstractLSMTest {
    private static BlobContainerClient client;

    private static BlobServiceClient blobServiceClient;
    private static final String MOCK_SERVER_REGION = "us-west-2";
    private static AzuriteContainer azBlob;
    public static final String AZURITE_CONTAINER_VER = "mcr.microsoft.com/azure-storage/azurite:3.35.0";

    public static void generateSelfSignedTLS() throws Exception {
        java.security.Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
        keyPairGenerator.initialize(2048);
        KeyPair keyPair = keyPairGenerator.generateKeyPair();
        PublicKey publicKey = keyPair.getPublic();
        PrivateKey privateKey = keyPair.getPrivate();
        long now = System.currentTimeMillis();
        Date startDate = new Date(now);
        X500Name dnName = new X500Name("CN=asterixdb azure test");
        BigInteger certSerialNumber = new BigInteger(Long.toString(now));
        Date endDate = new Date(now + 24 * 60 * 60 * 1000L); // 1 day validity
        SubjectPublicKeyInfo subjectPublicKeyInfo = SubjectPublicKeyInfo.getInstance(publicKey.getEncoded());
        X509v3CertificateBuilder certificateBuilder = new X509v3CertificateBuilder(dnName, certSerialNumber, startDate,
                endDate, dnName, subjectPublicKeyInfo);
        JcaContentSignerBuilder contentSignerBuilder = new JcaContentSignerBuilder("SHA256WithRSA");
        X509Certificate selfSignedCertificate = new JcaX509CertificateConverter().setProvider("BC")
                .getCertificate(certificateBuilder.build(contentSignerBuilder.build(privateKey)));
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        keyStore.load(null, null);
        keyStore.setKeyEntry("mytestcert", privateKey, "password".toCharArray(),
                new java.security.cert.Certificate[] { selfSignedCertificate });
        try (FileOutputStream fos = new FileOutputStream("target" + File.separator + "azure_test.pfx")) {
            keyStore.store(fos, "password".toCharArray());
        }
    }

    @BeforeClass
    public static void setup() throws Exception {
        LOGGER.info("LSMAzBlobStorageTest setup");
        generateSelfSignedTLS();
        MountableFile azureCert = MountableFile.forHostPath("target/azure_test.pfx");
        azBlob = new AzuriteContainer(AZURITE_CONTAINER_VER).withSsl(azureCert, "password");
        azBlob.start();
        SslContext insecureSslContext =
                SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build();
        blobServiceClient = new BlobServiceClientBuilder().connectionString(azBlob.getConnectionString())
                .httpClient(new NettyAsyncHttpClientBuilder(
                        HttpClient.create().secure(sslSpec -> sslSpec.sslContext(insecureSslContext).build())).build())
                .buildClient();
        // Start the test clean by deleting any residual data from previous tests
        blobServiceClient.deleteBlobContainerIfExists(PLAYGROUND_CONTAINER);
        client = blobServiceClient.createBlobContainerIfNotExists(PLAYGROUND_CONTAINER);

        LOGGER.info("Az Blob Client created successfully");
        int writeBufferSize = StorageUtil.getIntSizeInBytes(5, StorageUtil.StorageUnit.MEGABYTE);
        URI blobStore = URI.create(blobServiceClient.getAccountUrl());
        String endpoint = blobStore.getScheme() + "://" + blobStore.getAuthority() + "/devstoreaccount1";
        AzBlobStorageClientConfig config = new AzBlobStorageClientConfig(MOCK_SERVER_REGION, endpoint, "", false, 0,
                PLAYGROUND_CONTAINER, 1, 0, 0, writeBufferSize, true, null);
        CLOUD_CLIENT = new AzBlobStorageCloudClient(config, ICloudGuardian.NoOpCloudGuardian.INSTANCE);
    }

    private static void cleanup() {
        try {
            PagedIterable<BlobItem> blobItems = client.listBlobs(new ListBlobsOptions().setPrefix(""), null);
            // Delete all the contents of the container
            for (BlobItem blobItem : blobItems) {
                BlobClient blobClient = client.getBlobClient(blobItem.getName());
                blobClient.delete();
            }
            // Delete the container
            blobServiceClient.deleteBlobContainer(PLAYGROUND_CONTAINER);
        } catch (Exception ex) {
            // ignore
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        LOGGER.info("Shutdown Azurite");
        // Azure clients do not need explicit closure.
        cleanup();
        azBlob.stop();
        azBlob.close();
    }
}
