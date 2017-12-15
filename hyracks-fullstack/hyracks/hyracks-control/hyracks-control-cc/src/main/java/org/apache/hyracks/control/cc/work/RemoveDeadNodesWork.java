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
package org.apache.hyracks.control.cc.work;

import java.util.Collection;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.cluster.INodeManager;
import org.apache.hyracks.control.cc.job.IJobManager;
import org.apache.hyracks.control.cc.job.JobRun;
import org.apache.hyracks.control.common.work.AbstractWork;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RemoveDeadNodesWork extends AbstractWork {
    private static final Logger LOGGER = LogManager.getLogger();

    private final ClusterControllerService ccs;

    public RemoveDeadNodesWork(ClusterControllerService ccs) {
        this.ccs = ccs;
    }

    @Override
    public void run() {
        try {
            INodeManager nodeManager = ccs.getNodeManager();
            Pair<Collection<String>, Collection<JobId>> result = nodeManager.removeDeadNodes();
            Collection<String> deadNodes = result.getLeft();
            Collection<JobId> affectedJobIds = result.getRight();
            int size = affectedJobIds.size();
            if (size > 0) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Number of affected jobs: " + size);
                }
                IJobManager jobManager = ccs.getJobManager();
                for (JobId jobId : affectedJobIds) {
                    JobRun run = jobManager.get(jobId);
                    if (run != null) {
                        run.getExecutor().notifyNodeFailures(deadNodes);
                    }
                }
            }
            if (!deadNodes.isEmpty()) {
                ccs.getContext().notifyNodeFailure(deadNodes);
            }
        } catch (HyracksException e) {
            LOGGER.log(Level.WARN, "Uncaught exception on notifyNodeFailure", e);
        }
    }

    @Override
    public Level logLevel() {
        return Level.DEBUG;
    }
}
