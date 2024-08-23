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

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.messaging.api.ICcAddressedMessage;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.messaging.CCMessageBroker;
import org.apache.asterix.utils.StorageUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CalculateStorageSizeRequestMessage implements ICcAddressedMessage {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final long serialVersionUID = 1L;
    private static final int FAILED_CALCULATED_SIZE = -1;
    private final String nodeId;
    private final long reqId;
    private final String database;
    private final DataverseName dataverse;
    private final String collection;
    private final String index;

    public CalculateStorageSizeRequestMessage(String nodeId, long reqId, String database, DataverseName dataverse,
            String collection, String index) {
        this.nodeId = nodeId;
        this.reqId = reqId;
        this.database = database;
        this.dataverse = dataverse;
        this.collection = collection;
        this.index = index;
    }

    @Override
    public void handle(ICcApplicationContext appCtx) throws HyracksDataException {
        CCMessageBroker messageBroker = (CCMessageBroker) appCtx.getServiceContext().getMessageBroker();

        try {
            long size = StorageUtil.getCollectionSize(appCtx, database, dataverse, collection, index);
            CalculateStorageSizeResponseMessage response =
                    new CalculateStorageSizeResponseMessage(this.reqId, size, null);
            messageBroker.sendApplicationMessageToNC(response, nodeId);
        } catch (Exception ex) {
            LOGGER.info("Failed to process request", ex);
            try {
                CalculateStorageSizeResponseMessage response =
                        new CalculateStorageSizeResponseMessage(this.reqId, FAILED_CALCULATED_SIZE, ex);
                messageBroker.sendApplicationMessageToNC(response, nodeId);
            } catch (Exception ex2) {
                LOGGER.info("Failed to process request", ex2);
                throw HyracksDataException.create(ex2);
            }
        }
    }
}
