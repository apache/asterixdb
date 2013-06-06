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

import edu.uci.ics.hyracks.api.dataset.DatasetJobRecord.Status;
import edu.uci.ics.hyracks.api.dataset.ResultSetId;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.common.work.IResultCallback;
import edu.uci.ics.hyracks.control.common.work.SynchronizableWork;

public class GetResultStatusWork extends SynchronizableWork {
    private final ClusterControllerService ccs;

    private final JobId jobId;

    private final ResultSetId rsId;

    private final IResultCallback<Status> callback;

    public GetResultStatusWork(ClusterControllerService ccs, JobId jobId, ResultSetId rsId,
            IResultCallback<Status> callback) {
        this.ccs = ccs;
        this.jobId = jobId;
        this.rsId = rsId;
        this.callback = callback;
    }

    @Override
    public void doRun() {
        try {
            Status status = ccs.getDatasetDirectoryService().getResultStatus(jobId, rsId);
            callback.setValue(status);
        } catch (HyracksDataException e) {
            callback.setException(e);
        }
    }

    @Override
    public String toString() {
        return "JobId@" + jobId + " ResultSetId@" + rsId;
    }
}
