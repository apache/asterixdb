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
package org.apache.hyracks.util;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import javax.net.ssl.SSLEngine;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpHost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.conn.util.InetAddressUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NetworkUtil {

    private static final Logger LOGGER = LogManager.getLogger();

    private NetworkUtil() {
    }

    public static void configure(SocketChannel sc) throws IOException {
        sc.setOption(StandardSocketOptions.TCP_NODELAY, true);
        sc.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
    }

    public static void closeQuietly(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException e) {
                LOGGER.warn("Failed to close", e);
            }
        }
    }

    public static URI toUri(HttpHost host) throws URISyntaxException {
        return builderFrom(host).build();
    }

    public static URI toUri(HttpHost host, String path) throws URISyntaxException {
        return builderFrom(host).setPath(path).build();
    }

    public static URI appendUriPath(URI uri, String... pathSegments) throws URISyntaxException {
        URIBuilder builder = new URIBuilder(uri);
        List<String> path = builder.getPathSegments();
        if (path.isEmpty()) {
            path = new ArrayList<>(pathSegments.length);
        }
        Collections.addAll(path, pathSegments);
        builder.setPathSegments(path);
        return builder.build();
    }

    public static URIBuilder builderFrom(HttpHost host) {
        return new URIBuilder().setHost(host.getHostName()).setPort(host.getPort()).setScheme(host.getSchemeName());
    }

    public static String toHostPort(String host, String port) {
        return InetAddressUtils.isIPv6Address(host) ? "[" + host + "]:" + port : host + ":" + port;
    }

    public static String toHostPort(String host, int port) {
        return InetAddressUtils.isIPv6Address(host) ? "[" + host + "]:" + port : host + ":" + port;
    }

    public static String toHostPort(InetSocketAddress address) {
        return address != null ? toHostPort(address.getHostString(), address.getPort()) : null;
    }

    public static InetSocketAddress parseInetSocketAddress(String hostPortString) {
        int lastColon = hostPortString.lastIndexOf(':');
        String host = decodeIPv6LiteralHost(lastColon < 0 ? hostPortString : hostPortString.substring(0, lastColon));
        int port = lastColon < 0 ? 0 : Integer.parseInt(hostPortString.substring(lastColon + 1));
        return new InetSocketAddress(host, port);
    }

    public static InetSocketAddress toInetSocketAddress(String maybeLiteralHost, int port) {
        return new InetSocketAddress(decodeIPv6LiteralHost(maybeLiteralHost), port);
    }

    public static List<InetSocketAddress> parseInetSocketAddresses(String... hostPortStrings) {
        List<InetSocketAddress> hosts = new ArrayList<>();
        for (String node : hostPortStrings) {
            hosts.add(parseInetSocketAddress(node));
        }
        return hosts;
    }

    public static String defaultPort(String maybeHostPort, int defaultPort) {
        Pair<String, Integer> decoded = extractHostPort(maybeHostPort, defaultPort);
        return encodeIPv6LiteralHost(decoded.getLeft()) + ":" + decoded.getRight();
    }

    public static Pair<String, Integer> extractHostPort(String maybeHostPort, int defaultPort) {
        try {
            int lastColon = maybeHostPort.lastIndexOf(':');
            if (lastColon == -1) {
                return Pair.of(maybeHostPort, defaultPort);
            }
            int closeBracket = maybeHostPort.lastIndexOf(']');
            if (closeBracket == -1) {
                if (InetAddressUtils.isIPv6Address(maybeHostPort)) {
                    return Pair.of(maybeHostPort, defaultPort);
                }
                return Pair.of(maybeHostPort.substring(0, lastColon),
                        Integer.parseInt(maybeHostPort.substring(lastColon + 1)));
            } else if (closeBracket < lastColon) {
                return Pair.of(decodeIPv6LiteralHost(maybeHostPort.substring(0, lastColon)),
                        Integer.parseInt(maybeHostPort.substring(lastColon + 1)));
            }
            return Pair.of(decodeIPv6LiteralHost(maybeHostPort), defaultPort);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static String encodeIPv6LiteralHost(String hostname) {
        return InetAddressUtils.isIPv6Address(hostname) ? "[" + hostname + "]" : hostname;
    }

    public static String decodeIPv6LiteralHost(String hostname) {
        return hostname.length() > 0 && hostname.charAt(0) == '[' ? hostname.substring(1, hostname.length() - 1)
                : hostname;
    }

    public static ByteBuffer enlargeSslPacketBuffer(SSLEngine engine, ByteBuffer buffer) {
        return enlargeSslBuffer(buffer, engine.getSession().getPacketBufferSize());
    }

    public static ByteBuffer enlargeSslApplicationBuffer(SSLEngine engine, ByteBuffer buffer) {
        return enlargeSslBuffer(buffer, engine.getSession().getApplicationBufferSize());
    }

    public static ByteBuffer enlargeSslBuffer(ByteBuffer src, int sessionProposedCapacity) {
        final ByteBuffer enlargedBuffer;
        if (sessionProposedCapacity > src.capacity()) {
            enlargedBuffer = ByteBuffer.allocate(sessionProposedCapacity);
        } else {
            enlargedBuffer = ByteBuffer.allocate(src.capacity() * 2);
        }
        src.flip();
        enlargedBuffer.put(src);
        return enlargedBuffer;
    }

    public static InetSocketAddress ensureUnresolved(InetSocketAddress address) {
        return address.isUnresolved() ? address
                : InetSocketAddress.createUnresolved(address.getHostString(), address.getPort());
    }

    public static InetSocketAddress ensureResolved(InetSocketAddress address) {
        return address.isUnresolved() ? new InetSocketAddress(address.getHostString(), address.getPort()) : address;
    }

    public static InetSocketAddress refresh(InetSocketAddress original) {
        InetSocketAddress refreshed = new InetSocketAddress(original.getHostString(), original.getPort());
        if (!Objects.equals(original.getAddress(), refreshed.getAddress())) {
            LOGGER.warn("ip address updated on refresh (was: {}, now: {})", original.getAddress(),
                    refreshed.getAddress());
        }
        return refreshed;
    }

    public static InetSocketAddress toInetSocketAddress(HttpHost httpHost) {
        return new InetSocketAddress(httpHost.getAddress(), httpHost.getPort());
    }
}
