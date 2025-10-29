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
package org.apache.asterix.app.external;

import java.io.File;
import java.io.IOException;
import java.net.UnixDomainSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.hyracks.algebricks.common.utils.Pair;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.codec.http.HttpResponseStatus;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.HttpClientResponse;

@SuppressWarnings("squid:S134")
public class CloudUDFLibrarian implements IExternalUDFLibrarian {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private String sockPath;
    private final EventLoopGroup elg;

    public CloudUDFLibrarian(String sockPath) {
        this.sockPath = sockPath;
        //force nio even if epoll/kqueue are on classpath
        this.elg = new NioEventLoopGroup();
    }

    private static String createAuthHeader(Pair<String, String> credentials) {
        String auth = credentials.first + ":" + credentials.second;
        byte[] encodedAuth = Base64.getEncoder().encode(auth.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encodedAuth);
    }

    public void setCredentials(Pair<String, String> credentials) {
    }

    public void setAddress(String address) {
        this.sockPath = address;
    }

    @Override
    public void install(String path, String type, String libPath, Pair<String, String> credentials) throws Exception {
        File lib = new File(libPath);
        UnixDomainSocketAddress sockAddr = UnixDomainSocketAddress.of(sockPath);

        HttpClient hc = HttpClient.create().runOn(elg)
                .headers(h -> h.set("Authorization", createAuthHeader(credentials))).remoteAddress(() -> sockAddr);
        hc.post().uri(path)
                .sendForm((req, form) -> form.multipart(true).attr("type", type).file("data", lib,
                        "application/octet-stream"))
                .responseSingle((response, content) -> handleResponse(response, content)).block();
    }

    @Override
    public void uninstall(String path, Pair<String, String> credentials) throws IOException, AsterixException {
        UnixDomainSocketAddress sockAddr = UnixDomainSocketAddress.of(sockPath);
        HttpClient hc = HttpClient.create().runOn(elg)
                .headers(h -> h.set("Authorization", createAuthHeader(credentials))).remoteAddress(() -> sockAddr);
        hc.delete().uri(path).responseSingle((response, content) -> handleResponse(response, content)).block();
    }

    private Mono<Void> handleResponse(HttpClientResponse response, reactor.netty.ByteBufMono content) {
        return content.asString().defaultIfEmpty("").flatMap(body -> {
            int respCode = response.status().code();
            if (respCode == HttpResponseStatus.OK.code()) {
                return Mono.empty();
            }

            String errorMessage;
            if (respCode == HttpResponseStatus.INTERNAL_SERVER_ERROR.code()
                    || respCode == HttpResponseStatus.BAD_REQUEST.code()) {
                try {
                    errorMessage = OBJECT_MAPPER.readTree(body).get("error").asText();
                } catch (IOException e) {
                    errorMessage = "Failed to parse error response: " + body;
                }
            } else {
                errorMessage = response.status().toString();
            }
            return Mono.error(new AsterixException(errorMessage));
        });
    }

    @Override
    public SocketType getSocketType() {
        return SocketType.DOMAIN;
    }
}
