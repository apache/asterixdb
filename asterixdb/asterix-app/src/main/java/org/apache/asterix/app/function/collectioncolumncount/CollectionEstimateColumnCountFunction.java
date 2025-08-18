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

package org.apache.asterix.app.function.collectioncolumncount;

import static org.apache.asterix.app.message.ExecuteStatementRequestMessage.DEFAULT_NC_TIMEOUT_MILLIS;

import java.util.concurrent.TimeUnit;

import org.apache.asterix.app.message.GetEstimatedColumnCountRequestMessage;
import org.apache.asterix.app.message.GetEstimatedColumnCountResponseMessage;
import org.apache.asterix.common.messaging.api.INCMessageBroker;
import org.apache.asterix.common.messaging.api.MessageFuture;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.metadata.declared.AbstractDatasourceFunction;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.api.application.INCServiceContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CollectionEstimateColumnCountFunction extends AbstractDatasourceFunction {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final long serialVersionUID = 1L;

    private final String database;
    private final DataverseName dataverseName;
    private final String collection;

    CollectionEstimateColumnCountFunction(AlgebricksAbsolutePartitionConstraint locations, String database,
            DataverseName dataverseName, String collection) {
        super(locations);
        this.database = database;
        this.dataverseName = dataverseName;
        this.collection = collection;
    }

    @Override
    public IRecordReader<char[]> createRecordReader(IHyracksTaskContext ctx, int partition)
            throws HyracksDataException {
        INCServiceContext serviceCtx = ctx.getJobletContext().getServiceContext();
        INCMessageBroker messageBroker = (INCMessageBroker) serviceCtx.getMessageBroker();
        MessageFuture messageFuture = messageBroker.registerMessageFuture();
        long futureId = messageFuture.getFutureId();

        GetEstimatedColumnCountRequestMessage request = new GetEstimatedColumnCountRequestMessage(
                serviceCtx.getNodeId(), futureId, database, dataverseName, collection);
        try {
            messageBroker.sendMessageToPrimaryCC(request);
            GetEstimatedColumnCountResponseMessage response = (GetEstimatedColumnCountResponseMessage) messageFuture
                    .get(DEFAULT_NC_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            if (response.getFailure() != null) {
                throw HyracksDataException.create(response.getFailure());
            }
            return new CollectionEstimateColumnCountReader(response.getColumnCount());
        } catch (Exception e) {
            LOGGER.info("Could not get estimated column count", e);
            throw HyracksDataException.create(e);
        } finally {
            messageBroker.deregisterMessageFuture(futureId);
        }
    }
}
