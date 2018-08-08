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

import java.util.List;

import org.apache.hyracks.api.dataflow.TaskAttemptId;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.util.ExceptionUtils;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.job.IJobManager;
import org.apache.hyracks.control.cc.job.JobRun;
import org.apache.hyracks.control.cc.job.TaskAttempt;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TaskFailureWork extends AbstractTaskLifecycleWork {
    private static final Logger LOGGER = LogManager.getLogger();
    private final List<Exception> exceptions;

    public TaskFailureWork(ClusterControllerService ccs, JobId jobId, TaskAttemptId taId, String nodeId,
            List<Exception> exceptions) {
        super(ccs, jobId, taId, nodeId);
        this.exceptions = exceptions;
    }

    @Override
    protected void performEvent(TaskAttempt ta) {
        Exception ex = exceptions.get(0);
        LOGGER.log(ExceptionUtils.causedByInterrupt(ex) ? Level.DEBUG : Level.WARN,
                "Executing task failure work for " + this, ex);
        IJobManager jobManager = ccs.getJobManager();
        JobRun run = jobManager.get(jobId);
        if (run == null) {
            return;
        }
        ccs.getResultDirectoryService().reportJobFailure(jobId, exceptions);
        run.getExecutor().notifyTaskFailure(ta, exceptions);
    }

    @Override
    public String toString() {
        return getName() + ": [" + jobId + ":" + taId + ":" + nodeId + "]";
    }
}
