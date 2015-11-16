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

package org.apache.asterix.api.http.servlet;

import junit.framework.Assert;
import org.apache.asterix.common.config.AsterixBuildProperties;
import org.apache.asterix.om.util.AsterixAppContextInfo;
import org.apache.asterix.test.runtime.ExecutionTest;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.junit.Test;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;


import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;


@SuppressWarnings("deprecation")
public class VersionAPIServletTest {

    @Test
    public void testGet() throws Exception {
        // Starts test asterixdb cluster.
        ExecutionTest.setUp();

        // Configures a test version api servlet.
        VersionAPIServlet servlet = spy(new VersionAPIServlet());
        ServletConfig mockServletConfig = mock(ServletConfig.class);
        servlet.init(mockServletConfig);
        Map<String, String> propMap = new HashMap<String, String>();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintWriter outputWriter = new PrintWriter(outputStream);

        // Creates mocks.
        ServletContext mockContext = mock(ServletContext.class);
        AsterixAppContextInfo mockCtx = mock(AsterixAppContextInfo.class);
        HttpServletRequest mockRequest = mock(HttpServletRequest.class);
        IHyracksClientConnection mockHcc = mock(IHyracksClientConnection.class);
        HttpServletResponse mockResponse = mock(HttpServletResponse.class);
        AsterixBuildProperties mockProperties = mock(AsterixBuildProperties.class);

        // Sets up mock returns.
        when(servlet.getServletContext()).thenReturn(mockContext);
        when(mockResponse.getWriter()).thenReturn(outputWriter);
        when(mockContext.getAttribute(RESTAPIServlet.HYRACKS_CONNECTION_ATTR)).thenReturn(mockHcc);
        when(mockContext.getAttribute(VersionAPIServlet.ASTERIX_BUILD_PROP_ATTR)).thenReturn(mockCtx);
        when(mockCtx.getBuildProperties()).thenReturn(mockProperties);
        when(mockProperties.getAllProps()).thenReturn(propMap);

        propMap.put("git.build.user.email","foo@bar.baz");
        propMap.put("git.build.host","fulliautomatix");
        propMap.put("git.dirty","true");
        propMap.put("git.remote.origin.url","git@github.com:apache/incubator-asterixdb.git");
        propMap.put("git.closest.tag.name","asterix-0.8.7-incubating");
        propMap.put("git.commit.id.describe-short","asterix-0.8.7-incubating-19-dirty");
        propMap.put("git.commit.user.email","foo@bar.baz");
        propMap.put("git.commit.time","21.10.2015 @ 23:36:41 PDT");
        propMap.put("git.commit.message.full","ASTERIXDB-1045: fix log file reading during recovery\n\nChange-Id: Ic83ee1dd2d7ba88180c25f4ec6c7aa8d0a5a7162\nReviewed-on: https://asterix-gerrit.ics.uci.edu/465\nTested-by: Jenkins <jenkins@fulliautomatix.ics.uci.edu>");
        propMap.put("git.build.version","0.8.8-SNAPSHOT");
        propMap.put("git.commit.message.short","ASTERIXDB-1045: fix log file reading during recovery");
        propMap.put("git.commit.id.abbrev","e1dad19");
        propMap.put("git.branch","foo/bar");
        propMap.put("git.build.user.name","Asterix");
        propMap.put("git.closest.tag.commit.count","19");
        propMap.put("git.commit.id.describe","asterix-0.8.7-incubating-19-ge1dad19-dirty");
        propMap.put("git.commit.id","e1dad1984640517366a7e73e323c9de27b0676f7");
        propMap.put("git.tags","");
        propMap.put("git.build.time","22.10.2015 @ 17:11:07 PDT");
        propMap.put("git.commit.user.name","Obelix");

        // Calls VersionAPIServlet.formResponseObject.
        servlet.doGet(mockRequest, mockResponse);

        // Constructs the actual response.
        JSONTokener tokener = new JSONTokener(new InputStreamReader(
                new ByteArrayInputStream(outputStream.toByteArray())));
        JSONObject actualResponse = new JSONObject(tokener);
        JSONObject expectedResponse = new JSONObject(propMap);

        // Checks the response contains all the expected keys.
        Assert.assertEquals(actualResponse.toString(),expectedResponse.toString());

        // Tears down the asterixdb cluster.
        ExecutionTest.tearDown();
    }
}
