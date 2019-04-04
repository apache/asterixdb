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

import static org.apache.asterix.api.http.server.ServletConstants.ASTERIX_APP_CONTEXT_INFO_ATTR;
import static org.apache.asterix.api.http.server.ServletConstants.HYRACKS_CONNECTION_ATTR;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.asterix.api.http.server.VersionApiServlet;
import org.apache.asterix.common.config.BuildProperties;
import org.apache.asterix.runtime.utils.CcApplicationContext;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.util.internal.logging.Log4J2LoggerFactory;

public class VersionApiServletTest {
    static {
        // bootstrap netty with log4j2 logging
        io.netty.util.internal.logging.InternalLoggerFactory.setDefaultFactory(Log4J2LoggerFactory.INSTANCE);
    }

    @Test
    public void testGet() throws Exception {
        // Configures a test version api servlet.
        VersionApiServlet servlet = new VersionApiServlet(new ConcurrentHashMap<>(), new String[] { "/" });
        Map<String, String> propMap = new HashMap<>();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintWriter outputWriter = new PrintWriter(outputStream);

        // Creates mocks.
        CcApplicationContext mockCtx = mock(CcApplicationContext.class);
        IServletRequest mockRequest = mock(IServletRequest.class);
        IHyracksClientConnection mockHcc = mock(IHyracksClientConnection.class);
        IServletResponse mockResponse = mock(IServletResponse.class);
        BuildProperties mockProperties = mock(BuildProperties.class);
        FullHttpRequest mockHttpRequest = mock(FullHttpRequest.class);

        // Put stuff in let map
        servlet.ctx().put(HYRACKS_CONNECTION_ATTR, mockHcc);
        servlet.ctx().put(ASTERIX_APP_CONTEXT_INFO_ATTR, mockCtx);
        // Sets up mock returns.
        when(mockResponse.writer()).thenReturn(outputWriter);
        when(mockRequest.getHttpRequest()).thenReturn(mockHttpRequest);
        when(mockHttpRequest.method()).thenReturn(HttpMethod.GET);
        when(mockCtx.getBuildProperties()).thenReturn(mockProperties);
        when(mockProperties.getAllProps()).thenReturn(propMap);

        propMap.put("git.build.user.email", "foo@bar.baz");
        propMap.put("git.build.host", "fulliautomatix");
        propMap.put("git.dirty", "true");
        propMap.put("git.remote.origin.url", "git@github.com:apache/incubator-asterixdb.git");
        propMap.put("git.closest.tag.name", "asterix-0.8.7-incubating");
        propMap.put("git.commit.id.describe-short", "asterix-0.8.7-incubating-19-dirty");
        propMap.put("git.commit.user.email", "foo@bar.baz");
        propMap.put("git.commit.time", "21.10.2015 @ 23:36:41 PDT");
        propMap.put("git.commit.message.full",
                "ASTERIXDB-1045: fix log file reading during recovery\n\nChange-Id: Ic83ee1dd2d7ba88180c25f4ec6c7aa8d0a5a7162\nReviewed-on: https://asterix-gerrit.ics.uci.edu/465\nTested-by: Jenkins <jenkins@fulliautomatix.ics.uci.edu>");
        propMap.put("git.build.version", "0.8.8-SNAPSHOT");
        propMap.put("git.commit.message.short", "ASTERIXDB-1045: fix log file reading during recovery");
        propMap.put("git.commit.id.abbrev", "e1dad19");
        propMap.put("git.branch", "foo/bar");
        propMap.put("git.build.user.name", "Asterix");
        propMap.put("git.closest.tag.commit.count", "19");
        propMap.put("git.commit.id.describe", "asterix-0.8.7-incubating-19-ge1dad19-dirty");
        propMap.put("git.commit.id", "e1dad1984640517366a7e73e323c9de27b0676f7");
        propMap.put("git.tags", "");
        propMap.put("git.build.time", "22.10.2015 @ 17:11:07 PDT");
        propMap.put("git.commit.user.name", "Obelix");

        // Calls VersionAPIServlet.formResponseObject.
        servlet.handle(mockRequest, mockResponse);

        // Constructs the actual response.

        ObjectMapper om = new ObjectMapper();
        ObjectNode actualResponse = (ObjectNode) om.readTree(outputStream.toByteArray());
        ObjectNode expectedResponse = om.createObjectNode();
        for (Map.Entry<String, String> e : propMap.entrySet()) {
            expectedResponse.put(e.getKey(), e.getValue());
        }

        // Checks the response contains all the expected keys.
        Assert.assertEquals(actualResponse.toString(), expectedResponse.toString());
    }
}
