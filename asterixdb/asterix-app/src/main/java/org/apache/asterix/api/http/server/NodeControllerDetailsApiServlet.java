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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.cluster.ClusterPartition;
import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.utils.HttpUtil;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.handler.codec.http.HttpResponseStatus;

public class NodeControllerDetailsApiServlet extends ClusterApiServlet {

    private static final Logger LOGGER = Logger.getLogger(NodeControllerDetailsApiServlet.class.getName());

    public NodeControllerDetailsApiServlet(ICcApplicationContext appCtx, ConcurrentMap<String, Object> ctx,
            String... paths) {
        super(appCtx, ctx, paths);
    }

    @Override
    protected void get(IServletRequest request, IServletResponse response) throws IOException {
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
            HttpUtil.setContentType(response, HttpUtil.ContentType.APPLICATION_JSON, HttpUtil.Encoding.UTF8);
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

    protected ObjectNode fixupKeys(ObjectNode json) {
        // TODO (mblow): generate the keys with _ to begin with
        List<String> keys = new ArrayList<>();
        for (Iterator<String> iter = json.fieldNames(); iter.hasNext();) {
            keys.add(iter.next());
        }
        for (String key : keys) {
            String newKey = key.replace('-', '_');
            if (!newKey.equals(key)) {
                json.set(newKey, json.remove(key));
            }
        }
        return json;
    }

    protected ObjectNode processNodeStats(IHyracksClientConnection hcc, String node) throws Exception {
        final String details = hcc.getNodeDetailsJSON(node, true, false);
        if (details == null) {
            throw new IllegalArgumentException();
        }
        ObjectNode json = (ObjectNode) OBJECT_MAPPER.readTree(details);
        int index = json.get("rrd-ptr").asInt() - 1;
        json.remove("rrd-ptr");

        List<String> keys = new ArrayList<>();
        for (Iterator<String> iter = json.fieldNames(); iter.hasNext();) {
            keys.add(iter.next());
        }

        final ArrayNode gcNames = (ArrayNode) json.get("gc-names");
        final ArrayNode gcCollectionTimes = (ArrayNode) json.get("gc-collection-times");
        final ArrayNode gcCollectionCounts = (ArrayNode) json.get("gc-collection-counts");

        for (String key : keys) {
            if (key.startsWith("gc-")) {
                json.remove(key);
            } else {
                final JsonNode keyNode = json.get(key);
                if (keyNode instanceof ArrayNode) {
                    final ArrayNode valueArray = (ArrayNode) keyNode;
                    // fixup an index of -1 to the final element in the array (i.e. RRD_SIZE)
                    if (index == -1) {
                        index = valueArray.size() - 1;
                    }
                    final JsonNode value = valueArray.get(index);
                    json.remove(key);
                    json.set(key.replaceAll("s$", ""), value);
                }
            }
        }
        ArrayNode gcs = OBJECT_MAPPER.createArrayNode();

        for (int i = 0; i < gcNames.size(); i++) {
            ObjectNode gc = OBJECT_MAPPER.createObjectNode();
            gc.set("name", gcNames.get(i));
            gc.set("collection-time", gcCollectionTimes.get(i).get(index));
            gc.set("collection-count", gcCollectionCounts.get(i).get(index));
            fixupKeys(gc);
            gcs.add(gc);
        }
        json.set("gcs", gcs);

        return json;
    }

    private ObjectNode processNodeConfig(IHyracksClientConnection hcc, String node) throws Exception {
        String config = hcc.getNodeDetailsJSON(node, false, true);
        if (config == null) {
            throw new IllegalArgumentException();
        }
        return (ObjectNode) OBJECT_MAPPER.readTree(config);
    }

    private ObjectNode processNodeThreadDump(IHyracksClientConnection hcc, String node) throws Exception {
        if ("cc".equals(node)) {
            return OBJECT_MAPPER.createObjectNode();
        }
        String dump = hcc.getThreadDump(node);
        if (dump == null) {
            // check to see if this is a node that is simply down
            IClusterStateManager csm = appCtx.getClusterStateManager();
            ClusterPartition[] cp = csm.getNodePartitions(node);
            throw cp != null ? new IllegalStateException() : new IllegalArgumentException();
        }
        return (ObjectNode) OBJECT_MAPPER.readTree(dump);
    }
}
