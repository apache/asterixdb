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
package org.apache.hyracks.test.http.servlet;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentMap;

import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.AbstractServlet;
import org.apache.hyracks.http.server.utils.HttpUtil;
import org.apache.hyracks.test.http.HttpTestUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.handler.codec.http.HttpResponseStatus;

public class ChattyServlet extends AbstractServlet {
    private static final Logger LOGGER = LogManager.getLogger();
    private byte[] bytes;

    public ChattyServlet(ConcurrentMap<String, Object> ctx, String[] paths) {
        super(ctx, paths);
        String line = "I don't know when to stop talking\n";
        StringBuilder responseBuilder = new StringBuilder();
        for (int i = 0; i < 100000; i++) {
            responseBuilder.append(line);
        }
        String responseString = responseBuilder.toString();
        bytes = responseString.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    protected void post(IServletRequest request, IServletResponse response) throws Exception {
        get(request, response);
    }

    @Override
    protected void get(IServletRequest request, IServletResponse response) throws Exception {
        response.setStatus(HttpResponseStatus.OK);
        HttpUtil.setContentType(response, HttpUtil.ContentType.TEXT_HTML, request);
        LOGGER.log(Level.WARN, "I am about to flood you... and a single buffer is " + bytes.length + " bytes");
        for (int i = 0; i < 100; i++) {
            response.outputStream().write(bytes);
        }
        HttpTestUtil.printMemUsage();
    }
}
