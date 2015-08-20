/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.hyracks.control.cc.work;

import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.cc.job.JobRun;
import edu.uci.ics.hyracks.control.common.work.IResultCallback;
import edu.uci.ics.hyracks.control.common.work.SynchronizableWork;

public class GetJobStatusWork extends SynchronizableWork {
    private final ClusterControllerService ccs;
    private final JobId jobId;
    private final IResultCallback<JobStatus> callback;

    public GetJobStatusWork(ClusterControllerService ccs, JobId jobId, IResultCallback<JobStatus> callback) {
        this.ccs = ccs;
        this.jobId = jobId;
        this.callback = callback;
    }

    @Override
    protected void doRun() throws Exception {
        try {
            JobRun run = ccs.getActiveRunMap().get(jobId);
            if (run == null) {
                run = ccs.getRunMapArchive().get(jobId);
            }
            JobStatus status = run == null ? null : run.getStatus();
            callback.setValue(status);
        } catch (Exception e) {
            callback.setException(e);
        }
    }
}