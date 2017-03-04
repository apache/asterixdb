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
package org.apache.hyracks.tests.integration;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

class TestUtil {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 16001;

    static URI uri(String path) throws URISyntaxException {
        return new URI("http", null, HOST, PORT, path, null, null);
    }

    static InputStream httpGetAsInputStream(URI uri) throws URISyntaxException, IOException {
        HttpClient client = HttpClients.createMinimal();
        HttpResponse response = client.execute(new HttpGet(uri));
        return response.getEntity().getContent();
    }

    static String httpGetAsString(String path) throws URISyntaxException, IOException {
        return httpGetAsString(uri(path));
    }

    static String httpGetAsString(URI uri) throws URISyntaxException, IOException {
        InputStream resultStream = httpGetAsInputStream(uri);
        return IOUtils.toString(resultStream, Charset.defaultCharset());
    }

    static ObjectNode getResultAsJson(String resultStr) throws IOException {
        return new ObjectMapper().readValue(resultStr, ObjectNode.class);
    }

    static ObjectNode httpGetAsObject(String path) throws URISyntaxException, IOException {
        return getResultAsJson(httpGetAsString(path));
    }

    static ObjectNode httpGetAsObject(URI uri) throws URISyntaxException, IOException {
        return getResultAsJson(httpGetAsString(uri));
    }
}
