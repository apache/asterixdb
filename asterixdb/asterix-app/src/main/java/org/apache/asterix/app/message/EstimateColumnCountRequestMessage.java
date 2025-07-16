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
package org.apache.asterix.app.message;

import java.util.List;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.messaging.CcIdentifiedMessage;
import org.apache.asterix.common.messaging.api.INcAddressedMessage;
import org.apache.asterix.messaging.NCMessageBroker;
import org.apache.asterix.metadata.utils.DatasetPartitions;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.lsm.LSMColumnBTree;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

public class EstimateColumnCountRequestMessage extends CcIdentifiedMessage implements INcAddressedMessage {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final long serialVersionUID = 1L;
    private final long reqId;
    private final List<DatasetPartitions> datasetPartitions;

    public EstimateColumnCountRequestMessage(long reqId, List<DatasetPartitions> datasetPartitions) {
        this.reqId = reqId;
        this.datasetPartitions = datasetPartitions;
    }

    @Override
    public void handle(INcApplicationContext appCtx) throws HyracksDataException {
        INCServiceContext ctx = appCtx.getServiceContext();
        Int2IntMap columnCountMap = new Int2IntOpenHashMap();
        try {
            Thread.currentThread().setName(toString());
            for (DatasetPartitions partitions : datasetPartitions) {
                for (int partition : partitions.getPartitions()) {
                    IIndexDataflowHelper indexDataflowHelper = partitions.getPrimaryIndexDataflowHelperFactory()
                            .create(ctx, partition);
                    try (indexDataflowHelper) {
                        indexDataflowHelper.open();
                        LSMColumnBTree index = (LSMColumnBTree) indexDataflowHelper.getIndexInstance();
                        columnCountMap.put(partition, index.getNumberOfColumns());
                    }
                }
            }
            EstimateColumnCountResponseMessage response =
                    new EstimateColumnCountResponseMessage(reqId, columnCountMap, null);
            respond(appCtx, response);
        } catch (Exception e) {
            LOGGER.info("failed to get collection column count", e);
            EstimateColumnCountResponseMessage response = new EstimateColumnCountResponseMessage(reqId, columnCountMap, e);
            respond(appCtx, response);
        }
    }

    private void respond(INcApplicationContext appCtx, EstimateColumnCountResponseMessage response)
            throws HyracksDataException {
        NCMessageBroker messageBroker = (NCMessageBroker) appCtx.getServiceContext().getMessageBroker();
        try {
            messageBroker.sendMessageToPrimaryCC(response);
        } catch (Exception e) {
            LOGGER.info("failed to send collection column count to cc", e);
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public boolean isWhispered() {
        return true;
    }
}
