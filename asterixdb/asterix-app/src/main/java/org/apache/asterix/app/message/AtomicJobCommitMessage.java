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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.context.IndexInfo;
import org.apache.asterix.common.context.PrimaryIndexOperationTracker;
import org.apache.asterix.common.messaging.api.INcAddressedMessage;
import org.apache.asterix.messaging.NCMessageBroker;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;

/**
 * Message sent from CC to all NCs asking to commit an atomic statement/job.
 */
public class AtomicJobCommitMessage implements INcAddressedMessage {

    private static final long serialVersionUID = 1L;
    private final JobId jobId;
    private final List<Integer> datasetIds;

    public AtomicJobCommitMessage(JobId jobId, List<Integer> datasetIds) {
        this.jobId = jobId;
        this.datasetIds = datasetIds;
    }

    @Override
    public void handle(INcApplicationContext appCtx) throws HyracksDataException, InterruptedException {
        IDatasetLifecycleManager datasetLifecycleManager = appCtx.getDatasetLifecycleManager();
        ForkJoinPool commonPool = ForkJoinPool.commonPool();
        List<Future> futures = new ArrayList<>();
        for (Integer datasetId : datasetIds) {
            for (IndexInfo indexInfo : datasetLifecycleManager.getDatasetInfo(datasetId).getIndexes().values()) {
                if (indexInfo.getIndex().isPrimaryIndex()) {
                    futures.add(commonPool.submit(() -> {
                        try {
                            ((PrimaryIndexOperationTracker) indexInfo.getIndex().getOperationTracker()).commit();
                        } catch (HyracksDataException e) {
                            throw new RuntimeException(e);
                        }
                    }));
                }
            }
        }
        for (Future f : futures) {
            try {
                f.get();
            } catch (ExecutionException e) {
                futures.forEach(future -> future.cancel(true));
                throw HyracksDataException.create(e);
            }
        }
        AtomicJobCompletionMessage message =
                new AtomicJobCompletionMessage(jobId, appCtx.getServiceContext().getNodeId());
        NCMessageBroker mb = (NCMessageBroker) appCtx.getServiceContext().getMessageBroker();
        try {
            mb.sendRealTimeMessageToCC(jobId.getCcId(), message);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public String toString() {
        return "AtomicJobCommitMessage{" + "jobId=" + jobId + ", datasetIds=" + datasetIds + '}';
    }
}
