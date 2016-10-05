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
package org.apache.hyracks.server.test;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.logging.Logger;

import junit.framework.Assert;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.hyracks.server.process.HyracksVirtualCluster;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class NCServiceIT {

    private static final String TARGET_DIR = StringUtils
            .join(new String[] { ".", "target" }, File.separator);
    private static final String LOG_DIR = StringUtils
            .join(new String[] { TARGET_DIR, "failsafe-reports" }, File.separator);
    private static final String RESOURCE_DIR = StringUtils
            .join(new String[] { TARGET_DIR, "test-classes", "NCServiceIT" }, File.separator);
    private static final String APP_HOME = StringUtils
            .join(new String[] { TARGET_DIR, "appassembler" }, File.separator);
    private static final Logger LOGGER = Logger.getLogger(NCServiceIT.class.getName());

    private static HyracksVirtualCluster cluster = null;

    @BeforeClass
    public static void setUp() throws Exception {
        cluster = new HyracksVirtualCluster(new File(APP_HOME), null);
        cluster.addNCService(
                new File(RESOURCE_DIR, "nc-red.conf"),
                new File(LOG_DIR, "nc-red.log")
        );
        cluster.addNCService(
                new File(RESOURCE_DIR, "nc-blue.conf"),
                new File(LOG_DIR, "nc-blue.log")
        );

        try {
            Thread.sleep(2000);
        }
        catch (InterruptedException ignored) {
        }

        // Start CC
        cluster.start(
                new File(RESOURCE_DIR, "cc.conf"),
                new File(LOG_DIR, "cc.log")
        );

        try {
            Thread.sleep(10000);
        }
        catch (InterruptedException ignored) {
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

    @Test
    public void IsNodelistCorrect() throws Exception {
        // Ping the nodelist HTTP API
        String localhost = InetAddress.getLoopbackAddress().getHostAddress();
        String response = getHttp("http://" + localhost + ":12345/rest/nodes");
        JSONObject result = new JSONObject(response);
        JSONArray nodes = result.getJSONArray("result");
        int numNodes = nodes.length();
        Assert.assertEquals("Wrong number of nodes!", 2, numNodes);
        for (int i = 0; i < nodes.length(); i++) {
            JSONObject node = nodes.getJSONObject(i);
            String id = node.getString("node-id");
            if (id.equals("red") || id.equals("blue")) {
                continue;
            }
            Assert.fail("Unexpected node ID '" + id + "'!");
        }
    }

    public static void main(String[] args) throws Exception {
        try {
            setUp();
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.severe("TEST CASE(S) FAILED");
        } finally {
            tearDown();
        }
    }
}
