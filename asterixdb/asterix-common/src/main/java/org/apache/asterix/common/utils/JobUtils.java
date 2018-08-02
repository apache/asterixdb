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

package org.apache.asterix.common.utils;

import java.util.EnumSet;

import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;

public class JobUtils {

    public enum ProgressState {
        NO_PROGRESS,
        ADDED_PENDINGOP_RECORD_TO_METADATA
    }

    public static JobId runJob(IHyracksClientConnection hcc, JobSpecification spec, boolean waitForCompletion)
            throws Exception {
        return runJob(hcc, spec, EnumSet.noneOf(JobFlag.class), waitForCompletion);
    }

    public static JobId runJob(IHyracksClientConnection hcc, JobSpecification spec, EnumSet<JobFlag> jobFlags,
            boolean waitForCompletion) throws Exception {
        spec.setMaxReattempts(0);
        final JobId jobId = hcc.startJob(spec, jobFlags);
        if (waitForCompletion) {
            String nameBefore = Thread.currentThread().getName();
            try {
                Thread.currentThread().setName(nameBefore + " : WaitForCompletionForJobId: " + jobId);
                hcc.waitForCompletion(jobId);
            } finally {
                Thread.currentThread().setName(nameBefore);
            }
        }
        return jobId;
    }
}
