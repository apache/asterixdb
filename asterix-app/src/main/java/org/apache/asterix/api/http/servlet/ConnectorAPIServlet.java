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

package org.apache.asterix.api.http.servlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.asterix.feeds.CentralFeedManager;
import org.apache.asterix.metadata.MetadataManager;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.metadata.declared.AqlMetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.utils.DatasetUtils;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.util.FlushDatasetUtils;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.client.NodeControllerInfo;
import org.apache.hyracks.dataflow.std.file.FileSplit;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/***
 * The REST API that takes a dataverse name and a dataset name as the input
 * and returns an array of file splits (IP, file-path) of the dataset in LOSSLESS_JSON.
 * It is mostly used by external runtime, e.g., Pregelix or IMRU to pull data
 * in parallel from existing AsterixDB datasets.
 *
 * @author yingyi
 */
public class ConnectorAPIServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    private static final String HYRACKS_CONNECTION_ATTR = "org.apache.asterix.HYRACKS_CONNECTION";

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setContentType("text/html");
        response.setCharacterEncoding("utf-8");
        PrintWriter out = response.getWriter();
        try {
            JSONObject jsonResponse = new JSONObject();
            String dataverseName = request.getParameter("dataverseName");
            String datasetName = request.getParameter("datasetName");
            if (dataverseName == null || datasetName == null) {
                jsonResponse.put("error", "Parameter dataverseName or datasetName is null,");
                out.write(jsonResponse.toString());
                out.flush();
                return;
            }
            ServletContext context = getServletContext();

            IHyracksClientConnection hcc = null;
            synchronized (context) {
                hcc = (IHyracksClientConnection) context.getAttribute(HYRACKS_CONNECTION_ATTR);
            }

            // Metadata transaction begins.
            MetadataManager.INSTANCE.init();
            MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();

            // Retrieves file splits of the dataset.
            AqlMetadataProvider metadataProvider = new AqlMetadataProvider(null, CentralFeedManager.getInstance());
            metadataProvider.setMetadataTxnContext(mdTxnCtx);
            Dataset dataset = metadataProvider.findDataset(dataverseName, datasetName);
            if (dataset == null) {
                jsonResponse.put("error",
                        "Dataset " + datasetName + " does not exist in " + "dataverse " + dataverseName);
                out.write(jsonResponse.toString());
                out.flush();
                return;
            }
            boolean temp = dataset.getDatasetDetails().isTemp();
            FileSplit[] fileSplits = metadataProvider.splitsForDataset(mdTxnCtx, dataverseName, datasetName,
                    datasetName, temp);
            ARecordType recordType = (ARecordType) metadataProvider.findType(dataset.getItemTypeDataverseName(),
                    dataset.getItemTypeName());
            List<List<String>> primaryKeys = DatasetUtils.getPartitioningKeys(dataset);
            StringBuilder pkStrBuf = new StringBuilder();
            for (List<String> keys : primaryKeys) {
                for (String key : keys) {
                    pkStrBuf.append(key).append(",");
                }
            }
            pkStrBuf.delete(pkStrBuf.length() - 1, pkStrBuf.length());

            // Constructs the returned json object.
            formResponseObject(jsonResponse, fileSplits, recordType, pkStrBuf.toString(), temp,
                    hcc.getNodeControllerInfos());

            // Flush the cached contents of the dataset to file system.
            FlushDatasetUtils.flushDataset(hcc, metadataProvider, mdTxnCtx, dataverseName, datasetName, datasetName);

            // Metadata transaction commits.
            MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
            // Writes file splits.
            out.write(jsonResponse.toString());
            out.flush();
        } catch (Exception e) {
            e.printStackTrace();
            out.println(e.getMessage());
            out.flush();
            e.printStackTrace(out);
        }
    }

    private void formResponseObject(JSONObject jsonResponse, FileSplit[] fileSplits, ARecordType recordType,
            String primaryKeys, boolean temp, Map<String, NodeControllerInfo> nodeMap) throws Exception {
        JSONArray partititons = new JSONArray();
        // Whether the dataset is temp or not
        jsonResponse.put("temp", temp);
        // Adds a primary key.
        jsonResponse.put("keys", primaryKeys);
        // Adds record type.
        jsonResponse.put("type", recordType.toJSON());
        // Generates file partitions.
        for (FileSplit split : fileSplits) {
            String ipAddress = nodeMap.get(split.getNodeName()).getNetworkAddress().getAddress().toString();
            String path = split.getLocalFile().getFile().getAbsolutePath();
            FilePartition partition = new FilePartition(ipAddress, path, split.getIODeviceId());
            partititons.put(partition.toJSONObject());
        }
        // Generates the response object which contains the splits.
        jsonResponse.put("splits", partititons);
    }
}

class FilePartition {
    private final String ipAddress;
    private final String path;
    private final int ioDeviceId;

    public FilePartition(String ipAddress, String path, int ioDeviceId) {
        this.ipAddress = ipAddress;
        this.path = path;
        this.ioDeviceId = ioDeviceId;
    }

    public String getIPAddress() {
        return ipAddress;
    }

    public String getPath() {
        return path;
    }

    public int getIODeviceId() {
        return ioDeviceId;
    }

    @Override
    public String toString() {
        return ipAddress + ":" + path;
    }

    public JSONObject toJSONObject() throws JSONException {
        JSONObject partition = new JSONObject();
        partition.put("ip", ipAddress);
        partition.put("path", path);
        partition.put("ioDeviceId", ioDeviceId);
        return partition;
    }
}
