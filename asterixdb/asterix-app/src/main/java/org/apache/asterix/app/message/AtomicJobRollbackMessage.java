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

import static org.apache.hyracks.util.ExitUtil.EC_FAILED_TO_ROLLBACK_ATOMIC_STATEMENT;

import java.util.List;
import java.util.Map;

import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.messaging.api.INcAddressedMessage;
import org.apache.asterix.common.storage.IIndexCheckpointManager;
import org.apache.asterix.common.storage.IIndexCheckpointManagerProvider;
import org.apache.asterix.common.storage.IndexCheckpoint;
import org.apache.asterix.common.storage.ResourceReference;
import org.apache.asterix.messaging.NCMessageBroker;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponentId;
import org.apache.hyracks.util.ExitUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Message sent from CC to all NCs to rollback an atomic statement/job.
 */
public class AtomicJobRollbackMessage implements INcAddressedMessage {

    private static final long serialVersionUID = 1L;
    private final JobId jobId;
    private final List<Integer> datasetIds;
    private final Map<String, ILSMComponentId> componentIdMap;

    private static final Logger LOGGER = LogManager.getLogger();

    public AtomicJobRollbackMessage(JobId jobId, List<Integer> datasetIds,
            Map<String, ILSMComponentId> componentIdMap) {
        this.jobId = jobId;
        this.datasetIds = datasetIds;
        this.componentIdMap = componentIdMap;
    }

    @Override
    public void handle(INcApplicationContext appCtx) throws HyracksDataException, InterruptedException {
        IDatasetLifecycleManager datasetLifecycleManager = appCtx.getDatasetLifecycleManager();
        IIndexCheckpointManagerProvider indexCheckpointManagerProvider =
                datasetLifecycleManager.getIndexCheckpointManagerProvider();
        componentIdMap.forEach((k, v) -> {
            try {
                IIndexCheckpointManager checkpointManager =
                        indexCheckpointManagerProvider.get(ResourceReference.ofIndex(k));
                if (checkpointManager.getCheckpointCount() > 0) {
                    IndexCheckpoint checkpoint = checkpointManager.getLatest();
                    if (checkpoint.getLastComponentId() == v.getMaxId()) {
                        LOGGER.info("Removing checkpoint for resource {} for component id {}", k,
                                checkpoint.getLastComponentId());
                        checkpointManager.deleteLatest(v.getMaxId());
                    }
                }
            } catch (Exception e) {
                LOGGER.error("Error while rolling back atomic statement for {}, halting JVM", jobId);
                ExitUtil.halt(EC_FAILED_TO_ROLLBACK_ATOMIC_STATEMENT);
            }
        });
        AtomicJobRollbackCompleteMessage message =
                new AtomicJobRollbackCompleteMessage(jobId, appCtx.getServiceContext().getNodeId());
        NCMessageBroker mb = (NCMessageBroker) appCtx.getServiceContext().getMessageBroker();
        try {
            mb.sendRealTimeMessageToPrimaryCC(message);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return "AtomicJobRollbackMessage{" + "jobId=" + jobId + ", datasetIds=" + datasetIds + '}';
    }
}
