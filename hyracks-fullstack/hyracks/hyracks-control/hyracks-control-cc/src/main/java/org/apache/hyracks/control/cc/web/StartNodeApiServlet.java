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
package org.apache.hyracks.control.cc.web;

import java.io.PrintWriter;
import java.util.Date;
import java.util.concurrent.ConcurrentMap;

import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.AbstractServlet;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.netty.handler.codec.http.HttpResponseStatus;

public class StartNodeApiServlet extends AbstractServlet {
    private ClusterControllerService ccs;

    public StartNodeApiServlet(ConcurrentMap<String, Object> ctx, String[] paths, ClusterControllerService ccs) {
        super(ctx, paths);
        this.ccs = ccs;
    }

    @Override
    protected void post(IServletRequest request, IServletResponse response) {
        String nodeId = request.getParameter("node");
        response.setStatus(HttpResponseStatus.OK);
        ObjectMapper om = new ObjectMapper();
        ObjectNode jsonObject = om.createObjectNode();
        jsonObject.put("date", new Date().toString());
        jsonObject.put("status", ccs.startNC(nodeId));
        final PrintWriter writer = response.writer();
        writer.print(jsonObject.toString());
        writer.close();
    }

}
