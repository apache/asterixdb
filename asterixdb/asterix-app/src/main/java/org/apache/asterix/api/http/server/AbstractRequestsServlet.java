/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.api.http.server;

import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

import org.apache.asterix.common.api.IClientRequest;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.AbstractServlet;
import org.apache.hyracks.http.server.utils.HttpUtil;
import org.apache.hyracks.util.JSONUtil;

import com.fasterxml.jackson.databind.node.ArrayNode;

import io.netty.handler.codec.http.HttpResponseStatus;

public abstract class AbstractRequestsServlet extends AbstractServlet {

    protected final ICcApplicationContext appCtx;

    public AbstractRequestsServlet(ConcurrentMap<String, Object> ctx, ICcApplicationContext appCtx, String... paths) {
        super(ctx, paths);
        this.appCtx = appCtx;
    }

    @Override
    protected void get(IServletRequest request, IServletResponse response) throws Exception {
        ArrayNode requestsJson = JSONUtil.createArray();
        Collection<IClientRequest> requests = getRequests();
        for (IClientRequest req : requests) {
            requestsJson.add(req.asJson());
        }
        HttpUtil.setContentType(response, HttpUtil.ContentType.APPLICATION_JSON, request);
        response.setStatus(HttpResponseStatus.OK);
        JSONUtil.writeNode(response.writer(), requestsJson);
        response.writer().flush();
    }

    abstract Collection<IClientRequest> getRequests();

}
