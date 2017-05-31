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

import static org.apache.asterix.api.http.servlet.ServletConstants.HYRACKS_CONNECTION_ATTR;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.file.StorageComponentProvider;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Dataverse;
import org.apache.asterix.utils.RebalanceUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.AbstractServlet;
import org.apache.hyracks.http.server.utils.HttpUtil;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * This servlet processes dataset rebalance requests. It can:
 * - rebalance a given dataset;
 * - rebalance all datasets in a given dataverse;
 * - rebalance all non-metadata datasets.
 */
public class RebalanceApiServlet extends AbstractServlet {
    private static final Logger LOGGER = Logger.getLogger(RebalanceApiServlet.class.getName());
    private static final String METADATA = "Metadata";
    private final ICcApplicationContext appCtx;

    public RebalanceApiServlet(ConcurrentMap<String, Object> ctx, String[] paths, ICcApplicationContext appCtx) {
        super(ctx, paths);
        this.appCtx = appCtx;
    }

    @Override
    protected void post(IServletRequest request, IServletResponse response) {
        PrintWriter out = response.writer();
        ObjectMapper om = new ObjectMapper();
        ObjectNode jsonResponse = om.createObjectNode();
        try {
            // Sets the content type.
            HttpUtil.setContentType(response, HttpUtil.ContentType.APPLICATION_JSON, HttpUtil.Encoding.UTF8);

            // Gets dataverse, dataset, and target nodes for rebalance.
            String dataverseName = request.getParameter("dataverseName");
            String datasetName = request.getParameter("datasetName");
            String nodes = request.getParameter("nodes");

            // Parses and check target nodes.
            if (nodes == null) {
                sendResponse(out, jsonResponse, response, HttpResponseStatus.BAD_REQUEST,
                        "nodes are not given");
                return;
            }
            String nodesString = StringUtils.strip(nodes, "\"'").trim();
            String[] targetNodes = nodesString.split(",");
            if ("".equals(nodesString)) {
                sendResponse(out, jsonResponse, response, HttpResponseStatus.BAD_REQUEST,
                        "target nodes should not be empty");
                return;
            }

            // If a user gives parameter datasetName, she should give dataverseName as well.
            if (dataverseName == null && datasetName != null) {
                sendResponse(out, jsonResponse, response, HttpResponseStatus.BAD_REQUEST,
                        "to rebalance a particular dataset, the parameter dataverseName must be given");
                return;
            }

            // Does not allow rebalancing a metadata dataset.
            if (METADATA.equals(dataverseName)) {
                sendResponse(out, jsonResponse, response, HttpResponseStatus.BAD_REQUEST,
                        "cannot rebalance a metadata dataset");
                return;
            }

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
            sendResponse(out, jsonResponse, response, HttpResponseStatus.OK, "successful");
        } catch (Exception e) {
            sendResponse(out, jsonResponse, response, HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
            LOGGER.log(Level.WARNING, e.getMessage(), e);
        }
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
        MetadataProvider metadataProvider = new MetadataProvider(appCtx, null, new StorageComponentProvider());
        RebalanceUtil.rebalance(dataverseName, datasetName, new LinkedHashSet<>(Arrays.asList(targetNodes)),
                metadataProvider, hcc);
    }

    // Sends HTTP response to the request client.
    private void sendResponse(PrintWriter out, ObjectNode jsonResponse, IServletResponse response,
            HttpResponseStatus status, String message) {
        jsonResponse.put("results", message);
        response.setStatus(status);
        out.write(jsonResponse.toString());
    }
}
