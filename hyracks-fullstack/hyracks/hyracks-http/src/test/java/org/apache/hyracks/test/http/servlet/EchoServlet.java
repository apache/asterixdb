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

import java.util.concurrent.ConcurrentMap;

import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.AbstractServlet;
import org.apache.hyracks.http.server.utils.HttpUtil;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * A servlet that echos the received request body
 */
public class EchoServlet extends AbstractServlet {

    public EchoServlet(ConcurrentMap<String, Object> ctx, String... paths) {
        super(ctx, paths);
    }

    @Override
    protected void post(IServletRequest request, IServletResponse response) throws Exception {
        final String requestBody = HttpUtil.getRequestBody(request);
        response.setStatus(HttpResponseStatus.OK);
        HttpUtil.setContentType(response, HttpUtil.ContentType.TEXT_PLAIN, request);
        response.writer().write(requestBody);
    }
}
