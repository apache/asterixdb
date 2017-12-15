/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.active;

import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.transaction.management.service.transaction.TxnIdFactory;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.job.DeployedJobSpecId;
import org.apache.hyracks.api.job.JobId;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Provides functionality for running DeployedJobSpecs
 */
public class DeployedJobService {

    private static final Logger LOGGER = LogManager.getLogger();

    //To enable new Asterix TxnId for separate deployed job spec invocations
    private static final byte[] TRANSACTION_ID_PARAMETER_NAME = "TxnIdParameter".getBytes();

    //pool size one (only running one thread at a time)
    private static final int POOL_SIZE = 1;

    //Starts running a deployed job specification periodically with an interval of "duration" seconds
    public static ScheduledExecutorService startRepetitiveDeployedJobSpec(DeployedJobSpecId distributedId,
            IHyracksClientConnection hcc, long duration, Map<byte[], byte[]> jobParameters, EntityId entityId) {
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(POOL_SIZE);
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    if (!runRepetitiveDeployedJobSpec(distributedId, hcc, jobParameters, duration, entityId)) {
                        scheduledExecutorService.shutdown();
                    }
                } catch (Exception e) {
                    LOGGER.log(Level.ERROR, "Job Failed to run for " + entityId.getExtensionName() + " "
                            + entityId.getDataverse() + "." + entityId.getEntityName() + ".", e);
                }
            }
        }, duration, duration, TimeUnit.MILLISECONDS);
        return scheduledExecutorService;
    }

    public static boolean runRepetitiveDeployedJobSpec(DeployedJobSpecId distributedId, IHyracksClientConnection hcc,
            Map<byte[], byte[]> jobParameters, long duration, EntityId entityId) throws Exception {
        long executionMilliseconds = runDeployedJobSpec(distributedId, hcc, jobParameters, entityId);
        if (executionMilliseconds > duration && LOGGER.isErrorEnabled()) {
            LOGGER.log(Level.ERROR,
                    "Periodic job for " + entityId.getExtensionName() + " " + entityId.getDataverse() + "."
                            + entityId.getEntityName() + " was unable to meet the required period of " + duration
                            + " milliseconds. Actually took " + executionMilliseconds + " execution will shutdown"
                            + new Date());
            return false;
        }
        return true;
    }

    public synchronized static long runDeployedJobSpec(DeployedJobSpecId distributedId, IHyracksClientConnection hcc,
            Map<byte[], byte[]> jobParameters, EntityId entityId) throws Exception {
        JobId jobId;
        long startTime = Instant.now().toEpochMilli();

        //Add the Asterix Transaction Id to the map
        jobParameters.put(TRANSACTION_ID_PARAMETER_NAME, String.valueOf(TxnIdFactory.create().getId()).getBytes());
        jobId = hcc.startJob(distributedId, jobParameters);

        hcc.waitForCompletion(jobId);
        long executionMilliseconds = Instant.now().toEpochMilli() - startTime;

        LOGGER.log(Level.INFO,
                "Deployed Job execution completed for " + entityId.getExtensionName() + " " + entityId.getDataverse()
                        + "."
                        + entityId.getEntityName() + ". Took " + executionMilliseconds + " milliseconds ");

        return executionMilliseconds;

    }

    @Override
    public String toString() {
        return "DeployedJobSpecService";
    }

}
