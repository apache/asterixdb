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

import java.util.Arrays;

import org.apache.hyracks.api.dataset.DatasetDirectoryRecord;
import org.apache.hyracks.api.dataset.ResultSetId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.dataset.IDatasetDirectoryService;
import org.apache.hyracks.control.common.work.IResultCallback;
import org.apache.hyracks.control.common.work.SynchronizableWork;

public class GetResultPartitionLocationsWork extends SynchronizableWork {
    private final ClusterControllerService ccs;

    private final JobId jobId;

    private final ResultSetId rsId;

    private final DatasetDirectoryRecord[] knownRecords;

    private final IResultCallback<DatasetDirectoryRecord[]> callback;

    public GetResultPartitionLocationsWork(ClusterControllerService ccs, JobId jobId, ResultSetId rsId,
            DatasetDirectoryRecord[] knownRecords, IResultCallback<DatasetDirectoryRecord[]> callback) {
        this.ccs = ccs;
        this.jobId = jobId;
        this.rsId = rsId;
        this.knownRecords = knownRecords;
        this.callback = callback;
    }

    @Override
    public void doRun() {
        final IDatasetDirectoryService dds = ccs.getDatasetDirectoryService();
        ccs.getExecutor().execute(new Runnable() {
            @Override
            public void run() {
                try {
                    dds.getResultPartitionLocations(jobId, rsId, knownRecords, callback);
                } catch (HyracksDataException e) {
                    callback.setException(e);
                }
            }
        });
    }

    @Override
    public String toString() {
        return getName() + ": JobId@" + jobId + " ResultSetId@" + rsId + " Known@" + Arrays.toString(knownRecords);
    }
}
