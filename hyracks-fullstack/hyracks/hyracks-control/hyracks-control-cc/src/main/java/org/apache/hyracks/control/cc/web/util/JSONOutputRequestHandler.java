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
package org.apache.hyracks.control.cc.web.util;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.AbstractServlet;
import org.apache.hyracks.http.server.utils.HttpUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.netty.handler.codec.http.HttpResponseStatus;

public class JSONOutputRequestHandler extends AbstractServlet {

    private static final Logger LOGGER = LogManager.getLogger();
    private final IJSONOutputFunction fn;

    public JSONOutputRequestHandler(ConcurrentMap<String, Object> ctx, String[] paths, IJSONOutputFunction fn) {
        super(ctx, paths);
        this.fn = fn;
    }

    @Override
    protected void get(IServletRequest request, IServletResponse response) {
        String localPath = localPath(request);
        String servletPath = servletPath(request);
        String host = host(request);
        while (localPath.startsWith("/")) {
            localPath = localPath.substring(1);
        }
        String[] parts = localPath.split("/");

        ObjectNode result = invoke(response, host, servletPath, parts);
        if (result != null) {
            deliver(request, response, result);
        }
    }

    protected ObjectNode invoke(IServletResponse response, String host, String servletPath, String[] parts) {
        try {
            return fn.invoke(host, servletPath, parts);
        } catch (Exception e) {
            LOGGER.log(Level.WARN, "Exception invoking " + fn.getClass().getName(), e);
            response.setStatus(HttpResponseStatus.BAD_REQUEST);
            response.writer().print(e.getMessage());
        }
        return null;
    }

    protected void deliver(IServletRequest request, IServletResponse response, ObjectNode result) {
        try {
            HttpUtil.setContentType(response, HttpUtil.ContentType.APPLICATION_JSON, request);
            ObjectMapper om = new ObjectMapper();
            om.writer().writeValue(response.writer(), result);
            response.setStatus(HttpResponseStatus.OK);
        } catch (IOException e) {
            LOGGER.log(Level.WARN, "Exception delivering result in " + getClass().getName(), e);
            response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
            response.writer().print(e.getMessage());
        }
    }
}
