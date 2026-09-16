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
package org.apache.hyracks.ipc.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.security.auth.x500.X500Principal;

import org.apache.hyracks.api.network.INetworkSecurityManager;
import org.apache.hyracks.api.network.ISocketChannel;
import org.apache.hyracks.ipc.security.NetworkSecurityConfig;
import org.apache.hyracks.ipc.security.NetworkSecurityManager;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Validating the peer's chain against the trust store says only that a holder of a trusted certificate answered, not
 * that the node which was dialled did. These assert that a client channel also holds the peer to the identity its
 * certificate was issued to, so a certificate the cluster CA issued to another node is refused (ASTERIXDB-3851).
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "ASTERIXDB-3851")
public class SslPeerIdentityTest {

    private static final String DIALLED_HOST = "localhost";
    private static final String OTHER_HOST = "hyracks-nc2.example.com";
    private static final int UNUSED_PORT = 3099;
    private static final String PASSWORD = SslPeerIdentityTest.class.getSimpleName();
    private static final String SIGNATURE_ALGORITHM = "SHA256withRSA";

    private static INetworkSecurityManager client;
    private static INetworkSecurityManager unverifyingClient;
    private static INetworkSecurityManager dialledHost;
    private static INetworkSecurityManager otherHost;
    private static ExecutorService executor;

    @BeforeClass
    public static void setUp() throws Exception {
        final KeyPair caKeyPair = keyPair();
        final X509Certificate ca = certificate(caKeyPair, "Hyracks Test CA", null, caKeyPair.getPrivate(), null);
        // one CA, trusted by the client, which has issued a certificate to each of two nodes
        final KeyStore trustStore = emptyStore();
        trustStore.setCertificateEntry("ca", ca);
        client = securityManager(emptyStore(), trustStore, true);
        unverifyingClient = securityManager(emptyStore(), trustStore, false);
        dialledHost = securityManager(identity(DIALLED_HOST, ca, caKeyPair.getPrivate()), trustStore, true);
        otherHost = securityManager(identity(OTHER_HOST, ca, caKeyPair.getPrivate()), trustStore, true);
        executor = Executors.newSingleThreadExecutor();
    }

    @AfterClass
    public static void tearDown() {
        executor.shutdownNow();
    }

    @Test
    public void clientEngineIdentifiesThePeerItDialled() {
        final SSLEngine engine =
                client.newClientSSLEngine(InetSocketAddress.createUnresolved(DIALLED_HOST, UNUSED_PORT));
        assertEquals(NetworkSecurityManager.ENDPOINT_IDENTIFICATION_ALGORITHM,
                engine.getSSLParameters().getEndpointIdentificationAlgorithm());
        // the algorithm on its own identifies nothing; the name it matches is the peer the engine was created for
        assertEquals(DIALLED_HOST, engine.getPeerHost());
        assertEquals(UNUSED_PORT, engine.getPeerPort());
    }

    /**
     * A server is not the party doing the dialling, so it has no name to hold its peer to and must not ask for one.
     */
    @Test
    public void serverEngineDoesNotIdentifyTheClient() {
        assertNull(dialledHost.newServerSSLEngine().getSSLParameters().getEndpointIdentificationAlgorithm());
    }

    @Test
    public void certificateIssuedToTheDialledHostIsAccepted() throws Exception {
        assertTrue(handshake(client, dialledHost));
    }

    /**
     * The escape hatch for a deployment whose certificates cannot name the addresses its nodes are reached at. The
     * chain is still validated, so this is not the same as trusting anything.
     */
    @Test
    public void identityIsNotVerifiedWhenTheOptionIsDisabled() throws Exception {
        assertNull(unverifyingClient.newClientSSLEngine(InetSocketAddress.createUnresolved(DIALLED_HOST, UNUSED_PORT))
                .getSSLParameters().getEndpointIdentificationAlgorithm());
        assertTrue(handshake(unverifyingClient, otherHost));
    }

    @Test
    public void certificateIssuedToAnotherHostIsRefused() throws Exception {
        try {
            handshake(client, otherHost);
        } catch (IllegalStateException e) {
            final SSLException sslFailure = sslCause(e);
            assertTrue("expected an SSL failure, got " + e, sslFailure != null);
            assertTrue(sslFailure.getMessage(), sslFailure.getMessage().contains(DIALLED_HOST));
            return;
        }
        throw new AssertionError(
                "handshake accepted a certificate issued to " + OTHER_HOST + " while dialling " + DIALLED_HOST);
    }

    /**
     * Connects to a peer secured by {@code peer} over the loopback interface and completes the client's handshake.
     * The name the peer's certificate is checked against is the one the socket was opened to, {@link #DIALLED_HOST}.
     */
    private boolean handshake(INetworkSecurityManager dialler, INetworkSecurityManager peer) throws Exception {
        try (ServerSocketChannel listener = ServerSocketChannel.open()) {
            listener.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
            final int port = ((InetSocketAddress) listener.getLocalAddress()).getPort();
            try (SocketChannel clientSocket = SocketChannel.open(new InetSocketAddress(DIALLED_HOST, port));
                    SocketChannel peerSocket = listener.accept()) {
                clientSocket.configureBlocking(false);
                peerSocket.configureBlocking(false);
                final ISocketChannel peerChannel = peer.getSocketChannelFactory().createServerChannel(peerSocket);
                final ISocketChannel clientChannel =
                        dialler.getSocketChannelFactory().createClientChannel(clientSocket);
                // the peer's handshake fails alongside the client's whenever the client refuses its certificate; its
                // outcome is not what is under test, but it has to be running for the client's to make progress
                final Future<?> peerHandshake = executor.submit(() -> quietly(peerChannel));
                try {
                    return clientChannel.handshake();
                } finally {
                    peerHandshake.cancel(true);
                }
            }
        }
    }

    private static void quietly(ISocketChannel channel) {
        try {
            channel.handshake();
        } catch (Exception e) {
            // expected whenever the client refuses the certificate
        }
    }

    private static SSLException sslCause(Throwable t) {
        for (Throwable cause = t; cause != null; cause = cause.getCause()) {
            if (cause instanceof SSLException) {
                return (SSLException) cause;
            }
        }
        return null;
    }

    private static INetworkSecurityManager securityManager(KeyStore keyStore, KeyStore trustStore,
            boolean verifyPeerIdentity) {
        return new NetworkSecurityManager(
                NetworkSecurityConfig.of(true, verifyPeerIdentity, verifyPeerIdentity, keyStore, PASSWORD, trustStore));
    }

    /** A key store holding a certificate issued to {@code host} by {@code ca}, with {@code host} as its only SAN. */
    private static KeyStore identity(String host, X509Certificate ca, PrivateKey caKey) throws Exception {
        final KeyPair keyPair = keyPair();
        final X509Certificate certificate = certificate(keyPair, host, host, caKey, ca);
        final KeyStore keyStore = emptyStore();
        keyStore.setKeyEntry("identity", keyPair.getPrivate(), PASSWORD.toCharArray(),
                new Certificate[] { certificate, ca });
        return keyStore;
    }

    private static X509Certificate certificate(KeyPair keyPair, String commonName, String subjectAltName,
            PrivateKey issuerKey, X509Certificate issuer) throws Exception {
        final X500Principal subject = new X500Principal("CN=" + commonName);
        final Instant now = Instant.now();
        final JcaX509v3CertificateBuilder builder =
                new JcaX509v3CertificateBuilder(issuer != null ? issuer.getSubjectX500Principal() : subject,
                        BigInteger.valueOf(now.toEpochMilli()), Date.from(now.minus(Duration.ofDays(1))),
                        Date.from(now.plus(Duration.ofDays(365))), subject, keyPair.getPublic());
        builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(issuer == null));
        if (subjectAltName != null) {
            builder.addExtension(Extension.subjectAlternativeName, false,
                    new GeneralNames(new GeneralName(GeneralName.dNSName, subjectAltName)));
        }
        final ContentSigner signer = new JcaContentSignerBuilder(SIGNATURE_ALGORITHM).build(issuerKey);
        return new JcaX509CertificateConverter().getCertificate(builder.build(signer));
    }

    private static KeyPair keyPair() throws Exception {
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        return generator.generateKeyPair();
    }

    private static KeyStore emptyStore() throws Exception {
        final KeyStore store = KeyStore.getInstance(KeyStore.getDefaultType());
        store.load(null, null);
        return store;
    }
}
