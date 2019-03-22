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

import static org.apache.hyracks.util.NetworkUtil.toHostPort;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.replication.IPartitionReplica;
import org.apache.asterix.common.storage.IReplicaManager;
import org.apache.asterix.common.storage.ReplicaIdentifier;
import org.apache.asterix.common.storage.ResourceStorageStats;
import org.apache.asterix.transaction.management.resource.PersistentLocalResourceRepository;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.AbstractServlet;
import org.apache.hyracks.http.server.utils.HttpUtil;
import org.apache.hyracks.util.JSONUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.netty.handler.codec.http.HttpResponseStatus;

public class StorageApiServlet extends AbstractServlet {

    private static final Logger LOGGER = LogManager.getLogger();
    private final INcApplicationContext appCtx;

    public StorageApiServlet(ConcurrentMap<String, Object> ctx, INcApplicationContext appCtx, String... paths) {
        super(ctx, paths);
        this.appCtx = appCtx;
    }

    @Override
    protected void get(IServletRequest request, IServletResponse response) throws IOException {
        HttpUtil.setContentType(response, HttpUtil.ContentType.APPLICATION_JSON, request);
        PrintWriter responseWriter = response.writer();
        try {
            JsonNode json;
            response.setStatus(HttpResponseStatus.OK);
            final String path = localPath(request);
            if ("".equals(path)) {
                json = getStatus(p -> true);
            } else if (path.startsWith("/partition")) {
                json = getPartitionStatus(path);
            } else if (path.startsWith("/stats")) {
                json = getStats();
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
            case "/promote":
                processPromote(request, response);
                break;
            case "/release":
                processRelease(request, response);
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
        final IReplicaManager storageSubsystem = appCtx.getReplicaManager();
        final Set<Integer> partitions =
                storageSubsystem.getPartitions().stream().filter(predicate).collect(Collectors.toSet());
        for (Integer partition : partitions) {
            final ObjectNode partitionJson = OBJECT_MAPPER.createObjectNode();
            partitionJson.put("partition", partition);
            final List<IPartitionReplica> replicas = storageSubsystem.getReplicas(partition);
            ArrayNode replicasArray = OBJECT_MAPPER.createArrayNode();
            for (IPartitionReplica replica : replicas) {
                final ObjectNode replicaJson = OBJECT_MAPPER.createObjectNode();
                replicaJson.put("location", toHostPort(replica.getIdentifier().getLocation()));
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
        appCtx.getReplicaManager().addReplica(replicaIdentifier);
        response.setStatus(HttpResponseStatus.OK);
    }

    private void processRemoveReplica(IServletRequest request, IServletResponse response) {
        final ReplicaIdentifier replicaIdentifier = getReplicaIdentifier(request);
        if (replicaIdentifier == null) {
            response.setStatus(HttpResponseStatus.BAD_REQUEST);
            return;
        }
        appCtx.getReplicaManager().removeReplica(replicaIdentifier);
        response.setStatus(HttpResponseStatus.OK);
    }

    private ReplicaIdentifier getReplicaIdentifier(IServletRequest request) {
        final String partition = request.getParameter("partition");
        final String host = request.getParameter("host");
        final String port = request.getParameter("port");
        if (partition == null || host == null || port == null) {
            return null;
        }
        final InetSocketAddress replicaAddress = new InetSocketAddress(host, Integer.valueOf(port));
        return ReplicaIdentifier.of(Integer.valueOf(partition), replicaAddress);
    }

    private void processPromote(IServletRequest request, IServletResponse response) throws HyracksDataException {
        final String partition = request.getParameter("partition");
        if (partition == null) {
            response.setStatus(HttpResponseStatus.BAD_REQUEST);
            return;
        }
        appCtx.getReplicaManager().promote(Integer.valueOf(partition));
        response.setStatus(HttpResponseStatus.OK);
    }

    private void processRelease(IServletRequest request, IServletResponse response) throws HyracksDataException {
        final String partition = request.getParameter("partition");
        if (partition == null) {
            response.setStatus(HttpResponseStatus.BAD_REQUEST);
            return;
        }
        appCtx.getReplicaManager().release(Integer.valueOf(partition));
        response.setStatus(HttpResponseStatus.OK);
    }

    private JsonNode getStats() throws HyracksDataException {
        final PersistentLocalResourceRepository localResourceRepository =
                (PersistentLocalResourceRepository) appCtx.getLocalResourceRepository();
        final ArrayNode result = OBJECT_MAPPER.createArrayNode();
        final List<ResourceStorageStats> storageStats = localResourceRepository.getStorageStats();
        storageStats.stream().map(ResourceStorageStats::asJson).forEach(result::add);
        return result;
    }
}
