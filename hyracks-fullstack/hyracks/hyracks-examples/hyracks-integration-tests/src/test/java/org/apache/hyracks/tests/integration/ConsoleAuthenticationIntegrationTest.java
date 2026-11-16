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

import java.io.File;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Map;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.control.cc.BaseCCApplication;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.hyracks.http.server.BasicAuthServlet;
import org.apache.hyracks.ipc.impl.HyracksConnection;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ConsoleAuthenticationIntegrationTest {

    private static final String USER = "admin";
    private static final String PASSWORD = "secret";
    private static final String CONSOLE = "http://127.0.0.1:39102";

    @ClassRule
    public static final TemporaryFolder TMP = new TemporaryFolder(new File("target"));

    private static ClusterControllerService cc;
    private static NodeControllerService nc;
    private static CCConfig ccConfig;

    @BeforeClass
    public static void init() throws Exception {
        ccConfig = new CCConfig();
        ccConfig.setClientListenAddress("127.0.0.1");
        ccConfig.setClientListenPort(39100);
        ccConfig.setClusterListenAddress("127.0.0.1");
        ccConfig.setClusterListenPort(39101);
        ccConfig.setConsoleListenPort(39102);
        ccConfig.setRootDir(TMP.newFolder("cc").getAbsolutePath());
        cc = new ClusterControllerService(ccConfig,
                new AuthenticatedApplication(BasicAuthServlet.hashPassword(PASSWORD)));
        cc.start();

        NCConfig ncConfig = new NCConfig("nc1");
        ncConfig.setClusterAddress("localhost");
        ncConfig.setClusterPort(39101);
        ncConfig.setClusterListenAddress("127.0.0.1");
        ncConfig.setDataListenAddress("127.0.0.1");
        ncConfig.setResultListenAddress("127.0.0.1");
        ncConfig.setIODevices(new String[] { TMP.newFolder("device0").getAbsolutePath() });
        nc = new NodeControllerService(ncConfig);
        nc.start();
    }

    @AfterClass
    public static void deinit() throws Exception {
        nc.stop();
        cc.stop();
    }

    @Test
    public void rejectsUnauthenticatedRequests() throws Exception {
        Assert.assertEquals(401, execute(new HttpGet(CONSOLE + "/rest/nodes"), null));
        Assert.assertEquals(401, execute(new HttpPost(CONSOLE + "/rest/startnode?node=nc1"), null));
        Assert.assertEquals(401, execute(new HttpGet(CONSOLE + "/applications/app&lib.jar"), null));
    }

    @Test
    public void rejectsWrongPassword() throws Exception {
        Assert.assertEquals(401, execute(new HttpGet(CONSOLE + "/rest/nodes"),
                BasicAuthServlet.createAuthHeader(USER, PASSWORD + "-wrong")));
    }

    @Test
    public void acceptsAuthenticatedRequests() throws Exception {
        Assert.assertEquals(200,
                execute(new HttpGet(CONSOLE + "/rest/nodes"), BasicAuthServlet.createAuthHeader(USER, PASSWORD)));
    }

    @Test
    public void leavesStaticResourcesOpen() throws Exception {
        Assert.assertEquals(200, execute(new HttpGet(CONSOLE + "/static/file.txt"), null));
    }

    @Test
    public void deploysBinaryToNodeControllers() throws Exception {
        byte[] content = new byte[] { 1, 2, 3, 4 };
        File binary = TMP.newFile("deployed.jar");
        Files.write(binary.toPath(), content);
        HyracksConnection hcc =
                new HyracksConnection(ccConfig.getClientListenAddress(), ccConfig.getClientListenPort());
        try {
            hcc.setConsoleCredentials(USER, PASSWORD);
            DeploymentId deploymentId = hcc.deployBinary(Collections.singletonList(binary.getAbsolutePath()));
            File fetched = new File(nc.getServerContext().getBaseDir(),
                    "applications" + File.separator + deploymentId + File.separator + binary.getName());
            Assert.assertArrayEquals(content, Files.readAllBytes(fetched.toPath()));
            hcc.unDeployBinary(deploymentId);
        } finally {
            hcc.close();
        }
    }

    @Test
    public void refusesUnauthenticatedDeployment() throws Exception {
        File binary = TMP.newFile("refused.jar");
        HyracksConnection hcc =
                new HyracksConnection(ccConfig.getClientListenAddress(), ccConfig.getClientListenPort());
        try {
            Assert.assertThrows(HyracksException.class,
                    () -> hcc.deployBinary(Collections.singletonList(binary.getAbsolutePath())));
        } finally {
            hcc.close();
        }
    }

    private static int execute(HttpUriRequest request, String authHeader) throws Exception {
        if (authHeader != null) {
            request.setHeader(HttpHeaders.AUTHORIZATION, authHeader);
        }
        try (CloseableHttpClient client = HttpClients.createMinimal();
                CloseableHttpResponse response = client.execute(request)) {
            return response.getStatusLine().getStatusCode();
        }
    }

    private static class AuthenticatedApplication extends BaseCCApplication {
        private final Map<String, String> credentials;

        AuthenticatedApplication(String passwordHash) {
            credentials = Collections.singletonMap(USER, passwordHash);
        }

        @Override
        public Map<String, String> getConsoleCredentials() {
            return credentials;
        }
    }
}
