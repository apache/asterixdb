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

import static org.apache.asterix.api.http.server.ServletConstants.HYRACKS_CONNECTION_ATTR;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.ConcurrentMap;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.utils.HttpUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.node.ObjectNode;

import io.netty.handler.codec.http.HttpResponseStatus;

public class ClusterControllerDetailsApiServlet extends ClusterApiServlet {

    private static final Logger LOGGER = LogManager.getLogger();

    public ClusterControllerDetailsApiServlet(ICcApplicationContext appCtx, ConcurrentMap<String, Object> ctx,
            String... paths) {
        super(appCtx, ctx, paths);
    }

    @Override
    protected void get(IServletRequest request, IServletResponse response) throws IOException {
        HttpUtil.setContentType(response, HttpUtil.ContentType.APPLICATION_JSON, request);
        PrintWriter responseWriter = response.writer();
        IHyracksClientConnection hcc = (IHyracksClientConnection) ctx.get(HYRACKS_CONNECTION_ATTR);
        try {
            ObjectNode json;
            response.setStatus(HttpResponseStatus.OK);
            if ("".equals(localPath(request))) {
                json = (ObjectNode) getClusterStateJSON(request, "../").get("cc");
            } else {
                json = processNode(request, hcc);
            }
            responseWriter.write(OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(json));
        } catch (IllegalArgumentException e) { // NOSONAR - exception not logged or rethrown
            response.setStatus(HttpResponseStatus.NOT_FOUND);
        } catch (Exception e) {
            LOGGER.log(Level.INFO, "exception thrown for " + request, e);
            response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
            responseWriter.write(e.toString());
        }
        responseWriter.flush();
    }

    private ObjectNode processNode(IServletRequest request, IHyracksClientConnection hcc) throws Exception {
        String localPath = localPath(request);
        if (localPath.endsWith("/")) {
            throw new IllegalArgumentException();
        }
        String[] parts = localPath.substring(1).split("/");

        if ("".equals(localPath)) {
            return (ObjectNode) getClusterStateJSON(request, "../../").get("cc");
        } else if (parts.length == 1) {
            switch (parts[0]) {
                case "config":
                    return OBJECT_MAPPER.readValue(processNodeDetails(hcc, false, true), ObjectNode.class);
                case "stats":
                    return OBJECT_MAPPER.readValue(processNodeDetails(hcc, true, false), ObjectNode.class);
                case "threaddump":
                    return processCCThreadDump(hcc);

                default:
                    throw new IllegalArgumentException();
            }

        } else {
            throw new IllegalArgumentException();
        }
    }

    private String processNodeDetails(IHyracksClientConnection hcc, boolean includeStats, boolean includeConfig)
            throws Exception {
        final String details = hcc.getNodeDetailsJSON(null, includeStats, includeConfig);
        if (details == null) {
            throw new IllegalStateException("unable to retrieve details for CC");
        }
        return details;
    }

    private ObjectNode processCCThreadDump(IHyracksClientConnection hcc) throws Exception {
        String dump = hcc.getThreadDump(null);
        if (dump == null) {
            throw new IllegalStateException("unable to retrieve thread dump for CC");
        }
        return (ObjectNode) OBJECT_MAPPER.readTree(dump);
    }
}
