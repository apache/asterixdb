/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.control.cc.job.manager.events;

import java.util.UUID;

import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.job.IJobStatusConditionVariable;
import edu.uci.ics.hyracks.control.cc.jobqueue.SynchronizableRunnable;

public class GetJobStatusConditionVariableEvent extends SynchronizableRunnable {
    private final ClusterControllerService ccs;
    private final UUID jobId;
    private IJobStatusConditionVariable cVar;

    public GetJobStatusConditionVariableEvent(ClusterControllerService ccs, UUID jobId) {
        this.ccs = ccs;
        this.jobId = jobId;
    }

    @Override
    protected void doRun() throws Exception {
        cVar = ccs.getRunMap().get(jobId);
    }

    public IJobStatusConditionVariable getConditionVariable() {
        return cVar;
    }
}