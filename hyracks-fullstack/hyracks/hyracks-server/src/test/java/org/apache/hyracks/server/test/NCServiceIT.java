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

import junit.framework.Assert;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class NCServiceIT {

    private static final String RESOURCE_DIR = StringUtils
            .join(new String[]{System.getProperty("user.dir"), "src", "test", "resources", "NCServiceIT"},
                    File.separator);
    private static final String APP_DIR = StringUtils
            .join(new String[]{System.getProperty("user.dir"), "target", "appassembler", "bin"},
                    File.separator);
    private static final Logger LOGGER = Logger.getLogger(NCServiceIT.class.getName());
    private static List<Process> procs = new ArrayList<>();

    @BeforeClass
    public static void setUp() throws Exception {
        // Start two NC Services - don't read their output as they don't terminate
        procs.add(invoke(APP_DIR + File.separator + "hyracksncservice",
                "-config-file", RESOURCE_DIR + File.separator + "nc-red.conf",
                "-command", APP_DIR + File.separator + "hyracksnc"));
        procs.add(invoke(APP_DIR + File.separator + "hyracksncservice",
                "-config-file", RESOURCE_DIR + File.separator + "nc-blue.conf",
                "-command", APP_DIR + File.separator + "hyracksnc"));
        try {
            Thread.sleep(2000);
        }
        catch (InterruptedException ignored) {
        }

        // Start CC
        procs.add(invoke(APP_DIR + File.separator + "hyrackscc",
                "-config-file", RESOURCE_DIR + File.separator + "cc.conf"));
        try {
            Thread.sleep(10000);
        }
        catch (InterruptedException ignored) {
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        for (Process p : procs) {
            p.destroy();
            p.waitFor();
        }
    }

    private static String getHttp(String url) throws Exception {
        HttpClient client = new HttpClient();
        GetMethod get = new GetMethod(url);
        int statusCode;
        try {
            statusCode = client.executeMethod(get);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
        String response = get.getResponseBodyAsString();
        if (statusCode == HttpStatus.SC_OK) {
            return response;
        } else {
            throw new Exception("HTTP error " + statusCode + ":\n" + response);
        }
    }

    private static Process invoke(String... args) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(args);
        pb.redirectErrorStream(true);
        Process p = pb.start();
        return p;
    }

    @Test
    public void IsNodelistCorrect() throws Exception {
        // Ping the nodelist HTTP API
        String localhost = InetAddress.getLoopbackAddress().getHostAddress();
        String response = getHttp("http://" + localhost + ":12345/rest/nodes");
        JSONObject result = new JSONObject(response);
        JSONArray nodes = result.getJSONArray("result");
        int numNodes = nodes.length();
        Assert.assertEquals("Wrong number of nodes!", numNodes, 2);
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
