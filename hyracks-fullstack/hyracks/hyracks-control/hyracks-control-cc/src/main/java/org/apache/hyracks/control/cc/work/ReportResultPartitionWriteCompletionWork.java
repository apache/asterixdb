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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.result.ResultSetId;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.common.work.AbstractWork;

public class ReportResultPartitionWriteCompletionWork extends AbstractWork {
    private final ClusterControllerService ccs;

    private final JobId jobId;

    private final ResultSetId rsId;

    private final int partition;

    public ReportResultPartitionWriteCompletionWork(ClusterControllerService ccs, JobId jobId, ResultSetId rsId,
            int partition) {
        this.ccs = ccs;
        this.jobId = jobId;
        this.rsId = rsId;
        this.partition = partition;
    }

    @Override
    public void run() {
        try {
            ccs.getResultDirectoryService().reportResultPartitionWriteCompletion(jobId, rsId, partition);
        } catch (HyracksDataException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return getName() + ": JobId@" + jobId + " ResultSetId@" + rsId + " Partition@" + partition;
    }
}
