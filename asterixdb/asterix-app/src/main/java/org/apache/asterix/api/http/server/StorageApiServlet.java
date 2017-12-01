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

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.storage.IStorageSubsystem;
import org.apache.asterix.common.storage.PartitionReplica;
import org.apache.asterix.common.storage.ReplicaIdentifier;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.AbstractServlet;
import org.apache.hyracks.http.server.utils.HttpUtil;
import org.apache.hyracks.util.JSONUtil;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.netty.handler.codec.http.HttpResponseStatus;

public class StorageApiServlet extends AbstractServlet {

    private static final Logger LOGGER = Logger.getLogger(StorageApiServlet.class.getName());
    private final INcApplicationContext appCtx;

    public StorageApiServlet(ConcurrentMap<String, Object> ctx, INcApplicationContext appCtx, String... paths) {
        super(ctx, paths);
        this.appCtx = appCtx;
    }

    @Override
    protected void get(IServletRequest request, IServletResponse response) throws IOException {
        HttpUtil.setContentType(response, HttpUtil.ContentType.APPLICATION_JSON, HttpUtil.Encoding.UTF8);
        PrintWriter responseWriter = response.writer();
        try {
            JsonNode json;
            response.setStatus(HttpResponseStatus.OK);
            final String path = localPath(request);
            if ("".equals(path)) {
                json = getStatus(p -> true);
            } else if (path.startsWith("/partition")) {
                json = getPartitionStatus(path);
            } else {
                throw new IllegalArgumentException();
            }
            JSONUtil.writeNode(responseWriter, json);
        } catch (IllegalArgumentException e) {
            LOGGER.log(Level.INFO, "Unrecognized path: " + request, e);
            response.setStatus(HttpResponseStatus.NOT_FOUND);
        } catch (Exception e) {
            LOGGER.log(Level.INFO, "exception thrown for " + request, e);
            response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
            responseWriter.write(e.toString());
        }
        responseWriter.flush();
    }

    @Override
    protected void post(IServletRequest request, IServletResponse response) throws Exception {
        switch (localPath(request)) {
            case "/addReplica":
                processAddReplica(request, response);
                break;
            case "/removeReplica":
                processRemoveReplica(request, response);
                break;
            default:
                sendError(response, HttpResponseStatus.NOT_FOUND);
                break;
        }
    }

    private JsonNode getPartitionStatus(String path) {
        String[] token = path.split("/");
        if (token.length != 3) {
            throw new IllegalArgumentException();
        }
        // get the partition number from the path
        final Integer partition = Integer.valueOf(token[2]);
        return getStatus(partition::equals);
    }

    private JsonNode getStatus(Predicate<Integer> predicate) {
        final ArrayNode status = OBJECT_MAPPER.createArrayNode();
        final IStorageSubsystem storageSubsystem = appCtx.getStorageSubsystem();
        final Set<Integer> partitions =
                storageSubsystem.getPartitions().stream().filter(predicate).collect(Collectors.toSet());
        for (Integer partition : partitions) {
            final ObjectNode partitionJson = OBJECT_MAPPER.createObjectNode();
            partitionJson.put("partition", partition);
            final List<PartitionReplica> replicas = storageSubsystem.getReplicas(partition);
            ArrayNode replicasArray = OBJECT_MAPPER.createArrayNode();
            for (PartitionReplica replica : replicas) {
                final ObjectNode replicaJson = OBJECT_MAPPER.createObjectNode();
                replicaJson.put("location", replica.getIdentifier().getLocation().toString());
                replicaJson.put("status", replica.getStatus().toString());
                replicasArray.add(replicaJson);
            }
            partitionJson.set("replicas", replicasArray);
            status.add(partitionJson);
        }
        return status;
    }

    private void processAddReplica(IServletRequest request, IServletResponse response) {
        final ReplicaIdentifier replicaIdentifier = getReplicaIdentifier(request);
        if (replicaIdentifier == null) {
            response.setStatus(HttpResponseStatus.BAD_REQUEST);
            return;
        }
        appCtx.getStorageSubsystem().addReplica(replicaIdentifier);
        response.setStatus(HttpResponseStatus.OK);
    }

    private void processRemoveReplica(IServletRequest request, IServletResponse response) {
        final ReplicaIdentifier replicaIdentifier = getReplicaIdentifier(request);
        if (replicaIdentifier == null) {
            response.setStatus(HttpResponseStatus.BAD_REQUEST);
            return;
        }
        appCtx.getStorageSubsystem().removeReplica(replicaIdentifier);
        response.setStatus(HttpResponseStatus.OK);
    }

    private ReplicaIdentifier getReplicaIdentifier(IServletRequest request) {
        final String partition = request.getParameter("partition");
        final String host = request.getParameter("host");
        final String port = request.getParameter("port");
        if (partition == null || host == null || port == null) {
            return null;
        }
        final InetSocketAddress replicaAddress = InetSocketAddress.createUnresolved(host, Integer.valueOf(port));
        return ReplicaIdentifier.of(Integer.valueOf(partition), replicaAddress);
    }
}