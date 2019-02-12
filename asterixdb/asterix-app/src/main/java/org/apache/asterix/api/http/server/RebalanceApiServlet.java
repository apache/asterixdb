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

import java.io.PrintWriter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.asterix.app.active.ActiveNotificationHandler;
import org.apache.asterix.common.api.IMetadataLockManager;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.rebalance.NoOpDatasetRebalanceCallback;
import org.apache.asterix.utils.RebalanceUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.AbstractServlet;
import org.apache.hyracks.http.server.utils.HttpUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.node.ObjectNode;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * This servlet processes dataset rebalance requests. It can:
 * - rebalance a given dataset;
 * - rebalance all datasets in a given dataverse;
 * - rebalance all non-metadata datasets.
 */
public class RebalanceApiServlet extends AbstractServlet {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final String METADATA = "Metadata";
    private final ICcApplicationContext appCtx;

    // One-at-a-time thread executor, for rebalance tasks.
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    // A queue that maintains submitted rebalance requests.
    private final Queue<Future<Void>> rebalanceTasks = new ArrayDeque<>();

    // A queue that tracks the termination of rebalance threads.
    private final Queue<CountDownLatch> rebalanceFutureTerminated = new ArrayDeque<>();

    public RebalanceApiServlet(ConcurrentMap<String, Object> ctx, String[] paths, ICcApplicationContext appCtx) {
        super(ctx, paths);
        this.appCtx = appCtx;
    }

    @Override
    protected void delete(IServletRequest request, IServletResponse response) {
        try {
            // Sets the content type.
            HttpUtil.setContentType(response, HttpUtil.ContentType.APPLICATION_JSON, request);
            // Cancels all rebalance requests.
            cancelRebalance();
            // Sends the response back.
            sendResponse(response, HttpResponseStatus.OK, "rebalance tasks are cancelled");
        } catch (Exception e) {
            // Sends back and logs internal error if any exception happens during cancellation.
            sendResponse(response, HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage(), e);
        }

    }

    @Override
    protected void post(IServletRequest request, IServletResponse response) {
        try {
            // Gets dataverse, dataset, and target nodes for rebalance.
            String dataverseName = request.getParameter("dataverseName");
            String datasetName = request.getParameter("datasetName");
            String nodes = request.getParameter("nodes");

            // Parses and check target nodes.
            if (nodes == null) {
                sendResponse(response, HttpResponseStatus.BAD_REQUEST, "nodes are not given");
                return;
            }
            String nodesString = StringUtils.strip(nodes, "\"'").trim();
            String[] targetNodes = nodesString.split(",");
            if ("".equals(nodesString)) {
                sendResponse(response, HttpResponseStatus.BAD_REQUEST, "target nodes should not be empty");
                return;
            }

            // If a user gives parameter datasetName, she should give dataverseName as well.
            if (dataverseName == null && datasetName != null) {
                sendResponse(response, HttpResponseStatus.BAD_REQUEST,
                        "to rebalance a particular dataset, the parameter dataverseName must be given");
                return;
            }

            // Does not allow rebalancing a metadata dataset.
            if (METADATA.equals(dataverseName)) {
                sendResponse(response, HttpResponseStatus.BAD_REQUEST, "cannot rebalance a metadata dataset");
                return;
            }
            // Schedules a rebalance task and wait for its completion.
            CountDownLatch terminated = scheduleRebalance(dataverseName, datasetName, targetNodes, response);
            terminated.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            sendResponse(response, HttpResponseStatus.INTERNAL_SERVER_ERROR, "the rebalance service is interrupted", e);
        }
    }

    // Cancels all rebalance tasks.
    private synchronized void cancelRebalance() throws InterruptedException {
        for (Future<Void> rebalanceTask : rebalanceTasks) {
            rebalanceTask.cancel(true);
        }
    }

    // Removes a terminated task and its termination latch -- the heads.
    private synchronized void removeTermintedTask() {
        rebalanceTasks.remove();
        rebalanceFutureTerminated.remove();
    }

    // Schedules a rebalance task.
    private synchronized CountDownLatch scheduleRebalance(String dataverseName, String datasetName,
            String[] targetNodes, IServletResponse response) {
        CountDownLatch terminated = new CountDownLatch(1);
        Future<Void> task =
                executor.submit(() -> doRebalance(dataverseName, datasetName, targetNodes, response, terminated));
        rebalanceTasks.add(task);
        rebalanceFutureTerminated.add(terminated);
        return terminated;
    }

    // Performs the actual rebalance.
    private Void doRebalance(String dataverseName, String datasetName, String[] targetNodes, IServletResponse response,
            CountDownLatch terminated) {
        try {
            // Sets the content type.
            HttpUtil.setContentType(response, HttpUtil.ContentType.APPLICATION_JSON, HttpUtil.Encoding.UTF8);

            if (datasetName == null) {
                // Rebalances datasets in a given dataverse or all non-metadata datasets.
                List<Dataset> datasets = dataverseName == null ? getAllDatasetsForRebalance()
                        : getAllDatasetsForRebalance(dataverseName);
                for (Dataset dataset : datasets) {
                    // By the time rebalanceDataset(...) is called, the dataset could have been dropped.
                    // If that's the case, rebalanceDataset(...) would be a no-op.
                    rebalanceDataset(dataset.getDataverseName(), dataset.getDatasetName(), targetNodes);
                }
            } else {
                // Rebalances a given dataset from its current locations to the target nodes.
                rebalanceDataset(dataverseName, datasetName, targetNodes);
            }

            // Sends response.
            sendResponse(response, HttpResponseStatus.OK, "successful");
        } catch (InterruptedException e) {
            sendResponse(response, HttpResponseStatus.INTERNAL_SERVER_ERROR,
                    "the rebalance task is cancelled by a user", e);
        } catch (Exception e) {
            sendResponse(response, HttpResponseStatus.INTERNAL_SERVER_ERROR, e.toString(), e);
        } finally {
            // Removes the heads of the task queue and the latch queue.
            // Since the ExecutorService is one-at-a-time, the execution order of rebalance tasks is
            // the same as the request submission order.
            removeTermintedTask();
            // Notify that the rebalance task is terminated.
            terminated.countDown();
        }
        return null;
    }

    // Lists all datasets that should be rebalanced in a given datavserse.
    private List<Dataset> getAllDatasetsForRebalance(String dataverseName) throws Exception {
        List<Dataset> datasets;
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        try {
            datasets = getDatasetsInDataverseForRebalance(dataverseName, mdTxnCtx);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
            throw e;
        }
        return datasets;
    }

    // Lists all datasets that should be rebalanced.
    private List<Dataset> getAllDatasetsForRebalance() throws Exception {
        List<Dataset> datasets = new ArrayList<>();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        try {
            List<Dataverse> dataverses = MetadataManager.INSTANCE.getDataverses(mdTxnCtx);
            for (Dataverse dv : dataverses) {
                datasets.addAll(getDatasetsInDataverseForRebalance(dv.getDataverseName(), mdTxnCtx));
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
            throw e;
        }
        return datasets;
    }

    // Gets all datasets in a dataverse for the rebalance operation, with a given metadata transaction context.
    private List<Dataset> getDatasetsInDataverseForRebalance(String dvName, MetadataTransactionContext mdTxnCtx)
            throws Exception {
        return METADATA.equals(dvName) ? Collections.emptyList()
                : MetadataManager.INSTANCE.getDataverseDatasets(mdTxnCtx, dvName);
    }

    // Rebalances a given dataset.
    private void rebalanceDataset(String dataverseName, String datasetName, String[] targetNodes) throws Exception {
        IHyracksClientConnection hcc = (IHyracksClientConnection) ctx.get(HYRACKS_CONNECTION_ATTR);
        MetadataProvider metadataProvider = new MetadataProvider(appCtx, null);
        try {
            ActiveNotificationHandler activeNotificationHandler =
                    (ActiveNotificationHandler) appCtx.getActiveNotificationHandler();
            activeNotificationHandler.suspend(metadataProvider);
            try {
                IMetadataLockManager lockManager = appCtx.getMetadataLockManager();
                lockManager.acquireDatasetExclusiveModificationLock(metadataProvider.getLocks(),
                        dataverseName + '.' + datasetName);
                RebalanceUtil.rebalance(dataverseName, datasetName, new LinkedHashSet<>(Arrays.asList(targetNodes)),
                        metadataProvider, hcc, NoOpDatasetRebalanceCallback.INSTANCE);
            } finally {
                activeNotificationHandler.resume(metadataProvider);
            }
        } finally {
            metadataProvider.getLocks().unlock();
        }
    }

    // Sends HTTP response to the request client.
    private void sendResponse(IServletResponse response, HttpResponseStatus status, String message, Exception e) {
        if (status != HttpResponseStatus.OK) {
            if (e != null) {
                LOGGER.log(Level.WARN, message, e);
            } else {
                LOGGER.log(Level.WARN, message);
            }
        }
        PrintWriter out = response.writer();
        ObjectNode jsonResponse = OBJECT_MAPPER.createObjectNode();
        jsonResponse.put("results", message);
        response.setStatus(status);
        out.write(jsonResponse.toString());
    }

    // Sends HTTP response to the request client.
    private void sendResponse(IServletResponse response, HttpResponseStatus status, String message) {
        sendResponse(response, status, message, null);
    }
}
