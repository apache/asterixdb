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
package org.apache.hyracks.test.server;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Iterator;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.hyracks.test.server.process.HyracksVirtualCluster;
import org.apache.hyracks.util.file.FileUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

public class NCServiceIT {

    private static final String TARGET_DIR = FileUtil.joinPath(".", "target");
    private static final String LOG_DIR = FileUtil.joinPath(TARGET_DIR, "failsafe-reports");
    private static final String RESOURCE_DIR = FileUtil.joinPath(TARGET_DIR, "test-classes", "NCServiceIT");
    private static final String APP_HOME = FileUtil.joinPath(TARGET_DIR, "appassembler");
    private static final Logger LOGGER = LogManager.getLogger();

    private static HyracksVirtualCluster cluster = null;

    @BeforeClass
    public static void setUp() throws Exception {
        cluster = new HyracksVirtualCluster(new File(APP_HOME), null);

        cluster.addNCService(new File(RESOURCE_DIR, "nc-red.conf"), null);
        cluster.addNCService(new File(RESOURCE_DIR, "nc-blue.conf"), null);

        try {
            Thread.sleep(2000);
        } catch (InterruptedException ignored) {
        }

        // Start CC
        cluster.start(new File(RESOURCE_DIR, "cc.conf"), null);

        try {
            Thread.sleep(10000);
        } catch (InterruptedException ignored) {
        }
    }

    @AfterClass
    public static void tearDown() throws IOException {
        cluster.stop();
    }

    private static String getHttp(String url) throws Exception {
        HttpClient client = HttpClients.createDefault();
        HttpGet get = new HttpGet(url);
        int statusCode;
        final HttpResponse httpResponse;
        try {
            httpResponse = client.execute(get);
            statusCode = httpResponse.getStatusLine().getStatusCode();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
        String response = EntityUtils.toString(httpResponse.getEntity());
        if (statusCode == HttpStatus.SC_OK) {
            return response;
        } else {
            throw new Exception("HTTP error " + statusCode + ":\n" + response);
        }
    }

    private JsonNode getEndpoint(String endpoint) throws Exception {
        ObjectMapper om = new ObjectMapper();
        String localhost = InetAddress.getLoopbackAddress().getHostAddress();
        String response = getHttp("http://" + localhost + ":12345" + endpoint);
        JsonNode result = om.readTree(response);
        JsonNode nodes = result.get("result");
        return nodes;
    }

    @Test
    public void IsNodelistCorrect() throws Exception {
        // Ping the nodelist HTTP API

        JsonNode nodes = getEndpoint("/rest/nodes");
        int numNodes = nodes.size();
        Assert.assertEquals("Wrong number of nodes!", 2, numNodes);
        for (int i = 0; i < nodes.size(); i++) {
            JsonNode node = nodes.get(i);
            String id = node.get("node-id").asText();
            if (id.equals("red") || id.equals("blue")) {
                continue;
            }
            Assert.fail("Unexpected node ID '" + id + "'!");
        }
    }

    @Test
    public void isXmxOverrideCorrect() throws Exception {
        ArrayNode inputArgs = (ArrayNode) getEndpoint("/rest/nodes/red").get("input-arguments");
        for (Iterator<JsonNode> it = inputArgs.elements(); it.hasNext();) {
            String s = it.next().asText();
            if (s.startsWith("-Xmx") && s.endsWith("m")) {
                String digits = s.substring(4, 8);
                Assert.assertEquals("1234", digits);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        try {
            setUp();
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("TEST CASE(S) FAILED");
        } finally {
            tearDown();
        }
    }
}
