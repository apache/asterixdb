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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Proxy;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;

import org.apache.asterix.cloud.clients.profiler.NoOpRequestProfilerLimiter;
import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.cloud.io.ICloudProperties;
import org.apache.hyracks.util.StorageUtil;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.handler.ssl.OpenSsl;

/**
 * With SSL verification disabled the parallel downloader must accept any endpoint the synchronous client accepts.
 * The server here presents a certificate whose keyUsage omits digitalSignature: JDK TLS with a trust-all manager
 * takes it, while BoringSSL rejects it in the handshake with KEY_USAGE_BIT_INCORRECT whatever the trust manager says.
 */
public class S3ParallelDownloaderTlsTest {

    private static final byte[] BODY = "parallel downloader TLS test".getBytes(StandardCharsets.UTF_8);

    private static SSLServerSocket serverSocket;
    private static Thread serverThread;
    private static File scratch;

    @BeforeClass
    public static void setUp() throws Exception {
        scratch = Files.createTempDirectory("s3-tls-test").toFile();
        serverSocket = (SSLServerSocket) serverContext().getServerSocketFactory().createServerSocket(0, 50,
                InetAddress.getLoopbackAddress());
        serverThread = new Thread(S3ParallelDownloaderTlsTest::serve, "s3-tls-test-server");
        serverThread.setDaemon(true);
        serverThread.start();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        serverSocket.close();
        serverThread.join(5000);
        FileUtils.deleteQuietly(scratch);
    }

    @Test
    public void asyncClientAcceptsWhatTrustAllAccepts() throws Exception {
        // without netty-tcnative Netty already falls back to JDK TLS, and this test would prove nothing
        assertTrue("netty-tcnative should be on the test classpath", OpenSsl.isAvailable());
        S3ClientConfig config = S3ClientConfig.of(cloudProperties("https://127.0.0.1:" + serverSocket.getLocalPort()));
        FileReference target = new FileReference(new IODeviceHandle(scratch, null), "object.bin");
        try (S3ParallelDownloader downloader =
                new S3ParallelDownloader("bucket", null, config, NoOpRequestProfilerLimiter.INSTANCE)) {
            downloader.downloadFiles(Collections.singletonList(target));
        }
        assertArrayEquals(BODY, Files.readAllBytes(target.getFile().toPath()));
    }

    private static void serve() {
        while (!serverSocket.isClosed()) {
            try (SSLSocket socket = (SSLSocket) serverSocket.accept()) {
                BufferedReader in =
                        new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.US_ASCII));
                String line;
                do {
                    line = in.readLine();
                } while (line != null && !line.isEmpty());
                OutputStream out = socket.getOutputStream();
                out.write(("HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: " + BODY.length
                        + "\r\nETag: \"tls-test\"\r\nConnection: close\r\n\r\n").getBytes(StandardCharsets.US_ASCII));
                out.write(BODY);
                out.flush();
            } catch (Exception e) {
                // a rejected handshake or the socket closing at teardown; the client side reports the former
            }
        }
    }

    private static SSLContext serverContext() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        KeyPair keyPair = generator.generateKeyPair();
        X500Name name = new X500Name("CN=127.0.0.1");
        Instant now = Instant.now();
        JcaX509v3CertificateBuilder builder =
                new JcaX509v3CertificateBuilder(name, BigInteger.ONE, Date.from(now.minus(Duration.ofDays(1))),
                        Date.from(now.plus(Duration.ofDays(1))), name, keyPair.getPublic());
        builder.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.keyCertSign));
        builder.addExtension(Extension.subjectAlternativeName, false,
                new GeneralNames(new GeneralName(GeneralName.iPAddress, "127.0.0.1")));
        X509Certificate certificate = new JcaX509CertificateConverter().getCertificate(
                builder.build(new JcaContentSignerBuilder("SHA256withRSA").build(keyPair.getPrivate())));

        char[] password = "changeit".toCharArray();
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        keyStore.load(null, null);
        keyStore.setKeyEntry("server", keyPair.getPrivate(), password, new X509Certificate[] { certificate });
        // SunX509 hands out the key regardless of keyUsage; the default PKIX manager would decline to serve it
        KeyManagerFactory keyManagers = KeyManagerFactory.getInstance("SunX509");
        keyManagers.init(keyStore, password);
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(keyManagers.getKeyManagers(), null, null);
        return context;
    }

    private static ICloudProperties cloudProperties(String endpoint) {
        Map<String, Object> values = Map.of("getStorageEndpoint", endpoint, "getStorageRegion", "us-east-1",
                "getStoragePrefix", "", "getStorageCertificates", Collections.emptyList(), "isStorageDisableSSLVerify",
                true, "isStorageForcePathStyle", true, "getS3AccessKeyId", "access", "getS3SecretAccessKey", "secret",
                "getS3ParallelDownloaderClientType", "ASYNC","getWriteBufferSize",
                StorageUtil.getIntSizeInBytes(8, StorageUtil.StorageUnit.MEGABYTE));
        return (ICloudProperties) Proxy.newProxyInstance(ICloudProperties.class.getClassLoader(),
                new Class<?>[] { ICloudProperties.class }, (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return switch (method.getName()) {
                            case "equals" -> proxy == args[0];
                            case "hashCode" -> System.identityHashCode(proxy);
                            default -> "TLS test cloud properties";
                        };
                    }
                    if (values.containsKey(method.getName())) {
                        return values.get(method.getName());
                    }
                    Class<?> type = method.getReturnType();
                    if (type == boolean.class) {
                        return false;
                    }
                    if (type == int.class) {
                        return 0;
                    }
                    if (type == long.class) {
                        return 0L;
                    }
                    if (type == double.class) {
                        return 0.0;
                    }
                    return null;
                });
    }
}
