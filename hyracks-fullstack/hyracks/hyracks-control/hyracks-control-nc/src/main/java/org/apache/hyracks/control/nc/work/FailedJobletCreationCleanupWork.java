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
package org.apache.hyracks.control.nc.work;

import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.control.common.work.AbstractWork;
import org.apache.hyracks.control.nc.NodeControllerService;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FailedJobletCreationCleanupWork extends AbstractWork {

    private static final Logger LOGGER = LogManager.getLogger();

    private final NodeControllerService ncs;

    private final JobId jobId;

    public FailedJobletCreationCleanupWork(NodeControllerService ncs, JobId jobId) {
        this.ncs = ncs;
        this.jobId = jobId;
    }

    @Override
    public void run() {
        LOGGER.debug("Failed joblet creation cleaning up {}", jobId);
        try {
            ncs.removeJobParameterByteStore(jobId);
            ncs.getClusterController(jobId.getCcId()).notifyJobletCleanup(jobId, ncs.getId());
        } catch (Exception e) {
            LOGGER.info(e);
        }
    }

    @Override
    public String toString() {
        return getName() + " jobId:" + jobId;
    }

    @Override
    public Level logLevel() {
        return Level.TRACE;
    }
}
