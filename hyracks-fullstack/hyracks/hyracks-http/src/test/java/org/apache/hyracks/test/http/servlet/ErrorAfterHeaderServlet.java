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

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Flushes a chunk so the header goes out, and only then fails. What it writes after that is the error content, which
 * the chunked response delivers as the last chunk.
 */
public class ErrorAfterHeaderServlet extends AbstractServlet {

    public static final String CONTENT = "the results the client already received\n";
    public static final String ERROR = "{ \"error\": \"failed after the header was sent\" }";

    public ErrorAfterHeaderServlet(ConcurrentMap<String, Object> ctx, String[] paths) {
        super(ctx, paths);
    }

    @Override
    protected void get(IServletRequest request, IServletResponse response) throws Exception {
        response.setStatus(HttpResponseStatus.OK);
        HttpUtil.setContentType(response, HttpUtil.ContentType.TEXT_HTML, request);
        // enough content to exceed the chunk size, then flush so the header and the first chunk are sent
        byte[] content = CONTENT.getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < 100; i++) {
            response.outputStream().write(content);
        }
        response.outputStream().flush();
        // now fail: what is written from here on becomes the error content
        response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
        response.outputStream().write(ERROR.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    protected void post(IServletRequest request, IServletResponse response) throws Exception {
        get(request, response);
    }
}
