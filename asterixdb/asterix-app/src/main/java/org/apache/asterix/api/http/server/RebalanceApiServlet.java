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
import static org.apache.asterix.common.utils.IdentifierUtil.dataset;

import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.asterix.app.active.ActiveNotificationHandler;
import org.apache.asterix.common.api.IMetadataLockManager;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.MetadataConstants;
import org.apache.asterix.common.metadata.Namespace;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.metadata.utils.DatasetUtil;
import org.apache.asterix.rebalance.NoOpDatasetRebalanceCallback;
import org.apache.asterix.utils.RebalanceUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.IterableUtils;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
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
            Namespace namespace;
            try {
                namespace = ServletUtil.getNamespace(appCtx, request, "dataverseName");
            } catch (AlgebricksException e) {
                sendResponse(response, HttpResponseStatus.BAD_REQUEST, e.getMessage());
                return;
            }
            String datasetName = request.getParameter("datasetName");
            Set<String> targetNodes = new LinkedHashSet<>(request.getParameterValues("targetNode"));
            boolean forceRebalance = true;
            String force = request.getParameter("force");
            if (force != null) {
                forceRebalance = Boolean.parseBoolean(force);
            }
            // Parses and check target nodes.
            if (targetNodes.isEmpty()) {
                sendResponse(response, HttpResponseStatus.BAD_REQUEST, "at least one targetNode must be specified");
                return;
            }

            // If a user gives parameter datasetName, she should give dataverseName as well.
            if (namespace == null && datasetName != null) {
                sendResponse(response, HttpResponseStatus.BAD_REQUEST,
                        "to rebalance a particular " + dataset() + ", the parameter dataverseName must be given");
                return;
            }

            DataverseName dataverseName = null;
            String databaseName = null;
            if (namespace != null) {
                dataverseName = namespace.getDataverseName();
                databaseName = namespace.getDatabaseName();
            }
            //TODO(DB): also check System database
            // Does not allow rebalancing a metadata dataset.
            if (MetadataConstants.METADATA_DATAVERSE_NAME.equals(dataverseName)) {
                sendResponse(response, HttpResponseStatus.BAD_REQUEST, "cannot rebalance a metadata " + dataset());
                return;
            }
            // Schedules a rebalance task and wait for its completion.
            CountDownLatch terminated =
                    scheduleRebalance(databaseName, dataverseName, datasetName, targetNodes, response, forceRebalance);
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
    private synchronized CountDownLatch scheduleRebalance(String database, DataverseName dataverseName,
            String datasetName, Set<String> targetNodes, IServletResponse response, boolean force) {
        CountDownLatch terminated = new CountDownLatch(1);
        Future<Void> task = executor.submit(
                () -> doRebalance(database, dataverseName, datasetName, targetNodes, response, terminated, force));
        rebalanceTasks.add(task);
        rebalanceFutureTerminated.add(terminated);
        return terminated;
    }

    // Performs the actual rebalance.
    private Void doRebalance(String database, DataverseName dataverseName, String datasetName, Set<String> targetNodes,
            IServletResponse response, CountDownLatch terminated, boolean force) {
        try {
            // Sets the content type.
            HttpUtil.setContentType(response, HttpUtil.ContentType.APPLICATION_JSON, StandardCharsets.UTF_8);

            if (datasetName == null) {
                // Rebalances datasets in a given dataverse or all non-metadata datasets.
                Iterable<Dataset> datasets = dataverseName == null ? getAllDatasetsForRebalance()
                        : getAllDatasetsForRebalance(database, dataverseName);
                for (Dataset dataset : datasets) {
                    // By the time rebalanceDataset(...) is called, the dataset could have been dropped.
                    // If that's the case, rebalanceDataset(...) would be a no-op.
                    rebalanceDataset(dataset.getDatabaseName(), dataset.getDataverseName(), dataset.getDatasetName(),
                            targetNodes, force);
                }
            } else {
                // Rebalances a given dataset from its current locations to the target nodes.
                rebalanceDataset(database, dataverseName, datasetName, targetNodes, force);
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
    private Iterable<Dataset> getAllDatasetsForRebalance(String database, DataverseName dataverseName)
            throws Exception {
        Iterable<Dataset> datasets;
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        try {
            datasets = getDatasetsInDataverseForRebalance(database, dataverseName, mdTxnCtx);
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
            throw e;
        }
        return datasets;
    }

    // Lists all datasets that should be rebalanced.
    private Iterable<Dataset> getAllDatasetsForRebalance() throws Exception {
        List<Dataset> datasets = new ArrayList<>();
        MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
        try {
            List<Dataverse> dataverses = MetadataManager.INSTANCE.getDataverses(mdTxnCtx);
            for (Dataverse dv : dataverses) {
                CollectionUtils.addAll(datasets,
                        getDatasetsInDataverseForRebalance(dv.getDatabaseName(), dv.getDataverseName(), mdTxnCtx));
            }
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
        } catch (Exception e) {
            MetadataManager.INSTANCE.abortTransaction(mdTxnCtx);
            throw e;
        }
        return datasets;
    }

    // Gets all datasets in a dataverse for the rebalance operation, with a given metadata transaction context.
    private Iterable<Dataset> getDatasetsInDataverseForRebalance(String database, DataverseName dvName,
            MetadataTransactionContext mdTxnCtx) throws Exception {
        return MetadataConstants.METADATA_DATAVERSE_NAME.equals(dvName) ? Collections.emptyList()
                : IterableUtils.filteredIterable(
                        MetadataManager.INSTANCE.getDataverseDatasets(mdTxnCtx, database, dvName),
                        DatasetUtil::isNotView);
    }

    // Rebalances a given dataset.
    private void rebalanceDataset(String database, DataverseName dataverseName, String datasetName,
            Set<String> targetNodes, boolean force) throws Exception {
        IHyracksClientConnection hcc = (IHyracksClientConnection) ctx.get(HYRACKS_CONNECTION_ATTR);
        MetadataProvider metadataProvider = MetadataProvider.createWithDefaultNamespace(appCtx);
        try {
            ActiveNotificationHandler activeNotificationHandler =
                    (ActiveNotificationHandler) appCtx.getActiveNotificationHandler();
            activeNotificationHandler.suspend(metadataProvider, "rebalance api");
            try {
                IMetadataLockManager lockManager = appCtx.getMetadataLockManager();
                lockManager.acquireDatasetExclusiveModificationLock(metadataProvider.getLocks(), database,
                        dataverseName, datasetName);
                RebalanceUtil.rebalance(database, dataverseName, datasetName, targetNodes, metadataProvider, hcc,
                        NoOpDatasetRebalanceCallback.INSTANCE, force);
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
