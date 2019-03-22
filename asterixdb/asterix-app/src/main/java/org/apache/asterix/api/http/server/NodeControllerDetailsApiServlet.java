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

import static org.apache.asterix.api.http.server.NodeControllerDetailsHelper.fixupKeys;
import static org.apache.asterix.api.http.server.NodeControllerDetailsHelper.processNodeDetailsJSON;
import static org.apache.asterix.api.http.server.ServletConstants.HYRACKS_CONNECTION_ATTR;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.ConcurrentMap;

import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.utils.HttpUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.netty.handler.codec.http.HttpResponseStatus;

public class NodeControllerDetailsApiServlet extends ClusterApiServlet {

    private static final Logger LOGGER = LogManager.getLogger();

    public NodeControllerDetailsApiServlet(ICcApplicationContext appCtx, ConcurrentMap<String, Object> ctx,
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
                json = OBJECT_MAPPER.createObjectNode();
                json.set("ncs", getClusterStateJSON(request, "../").get("ncs"));
            } else {
                json = processNode(request, hcc);
            }
            responseWriter.write(OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(json));
        } catch (IllegalStateException e) { // NOSONAR - exception not logged or rethrown
            response.setStatus(HttpResponseStatus.SERVICE_UNAVAILABLE);
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
        final String node = parts[0];

        if (parts.length == 1) {
            ArrayNode ncs = (ArrayNode) getClusterStateJSON(request, "../../").get("ncs");
            for (int i = 0; i < ncs.size(); i++) {
                if (node.equals(ncs.get(i).get("node_id").asText())) {
                    return (ObjectNode) ncs.get(i);
                }
            }
            if ("cc".equals(node)) {
                return OBJECT_MAPPER.createObjectNode();
            }
            throw new IllegalArgumentException();
        } else if (parts.length == 2) {
            ObjectNode json;

            switch (parts[1]) {
                case "config":
                    json = processNodeConfig(hcc, node);
                    break;

                case "stats":
                    json = processNodeStats(hcc, node);
                    break;

                case "threaddump":
                    return processNodeThreadDump(hcc, node);

                default:
                    throw new IllegalArgumentException();
            }
            fixupKeys(json);

            return json;
        } else {
            throw new IllegalArgumentException();
        }
    }

    protected ObjectNode processNodeStats(IHyracksClientConnection hcc, String node) throws Exception {
        final String details = checkNullDetail(node, hcc.getNodeDetailsJSON(node, true, false));
        return processNodeDetailsJSON((ObjectNode) OBJECT_MAPPER.readTree(details), OBJECT_MAPPER);
    }

    private ObjectNode processNodeConfig(IHyracksClientConnection hcc, String node) throws Exception {
        String config = checkNullDetail(node, hcc.getNodeDetailsJSON(node, false, true));
        return (ObjectNode) OBJECT_MAPPER.readTree(config);
    }

    private ObjectNode processNodeThreadDump(IHyracksClientConnection hcc, String node) throws Exception {
        if ("cc".equals(node)) {
            return OBJECT_MAPPER.createObjectNode();
        }
        String dump = checkNullDetail(node, hcc.getThreadDump(node));
        return (ObjectNode) OBJECT_MAPPER.readTree(dump);
    }

    protected String checkNullDetail(String node, String value) {
        if (value != null) {
            return value;
        }
        if (node == null) {
            // something is seriously wrong if we can't get the cc detail
            throw new IllegalStateException("unable to obtain detail from cc");
        }
        // check to see if this is a node that is simply down
        IClusterStateManager csm = appCtx.getClusterStateManager();
        ClusterPartition[] cp = csm.getNodePartitions(node);
        throw cp != null ? new IllegalStateException("unable to obtain detail from node " + node)
                : new IllegalArgumentException("unknown node " + node);
    }
}
