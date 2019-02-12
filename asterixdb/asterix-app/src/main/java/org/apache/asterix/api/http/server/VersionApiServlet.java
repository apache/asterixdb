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
package org.apache.asterix.api.http.server;

import static org.apache.asterix.api.http.server.ServletConstants.ASTERIX_APP_CONTEXT_INFO_ATTR;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.AbstractServlet;
import org.apache.hyracks.http.server.utils.HttpUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.node.ObjectNode;

import io.netty.handler.codec.http.HttpResponseStatus;

public class VersionApiServlet extends AbstractServlet {
    private static final Logger LOGGER = LogManager.getLogger();

    public VersionApiServlet(ConcurrentMap<String, Object> ctx, String[] paths) {
        super(ctx, paths);
    }

    @Override
    protected void get(IServletRequest request, IServletResponse response) {
        response.setStatus(HttpResponseStatus.OK);
        ICcApplicationContext props = (ICcApplicationContext) ctx.get(ASTERIX_APP_CONTEXT_INFO_ATTR);
        Map<String, String> buildProperties = props.getBuildProperties().getAllProps();
        ObjectNode responseObject = OBJECT_MAPPER.createObjectNode();
        buildProperties.forEach(responseObject::put);
        try {
            HttpUtil.setContentType(response, HttpUtil.ContentType.TEXT_PLAIN, request);
        } catch (IOException e) {
            LOGGER.log(Level.WARN, "Failure handling request", e);
            response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
            return;
        }
        PrintWriter responseWriter = response.writer();
        responseWriter.write(responseObject.toString());
        responseWriter.flush();
    }

}
