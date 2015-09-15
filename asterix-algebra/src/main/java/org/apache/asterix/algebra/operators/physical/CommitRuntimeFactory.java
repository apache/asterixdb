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

package org.apache.asterix.algebra.operators.physical;

import org.apache.asterix.common.transactions.JobId;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntime;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;

public class CommitRuntimeFactory implements IPushRuntimeFactory {

    private static final long serialVersionUID = 1L;

    private final JobId jobId;
    private final int datasetId;
    private final int[] primaryKeyFields;
    private final boolean isTemporaryDatasetWriteJob;
    private final boolean isWriteTransaction;

    public CommitRuntimeFactory(JobId jobId, int datasetId, int[] primaryKeyFields, boolean isTemporaryDatasetWriteJob,
            boolean isWriteTransaction) {
        this.jobId = jobId;
        this.datasetId = datasetId;
        this.primaryKeyFields = primaryKeyFields;
        this.isTemporaryDatasetWriteJob = isTemporaryDatasetWriteJob;
        this.isWriteTransaction = isWriteTransaction;
    }

    @Override
    public String toString() {
        return "commit";
    }

    @Override
    public IPushRuntime createPushRuntime(IHyracksTaskContext ctx) throws AlgebricksException {
        return new CommitRuntime(ctx, jobId, datasetId, primaryKeyFields, isTemporaryDatasetWriteJob,
                isWriteTransaction);
    }
}
