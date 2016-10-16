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

import java.nio.ByteBuffer;

import org.apache.asterix.common.transactions.JobId;
import org.apache.asterix.common.transactions.LogType;
import org.apache.asterix.common.utils.TransactionUtil;
import org.apache.asterix.dataflow.data.nontagged.serde.ABooleanSerializerDeserializer;
import org.apache.hyracks.api.context.IHyracksTaskContext;

public class UpsertCommitRuntime extends CommitRuntime {
    private final int upsertIdx;

    public UpsertCommitRuntime(IHyracksTaskContext ctx, JobId jobId, int datasetId, int[] primaryKeyFields,
            boolean isTemporaryDatasetWriteJob, boolean isWriteTransaction, int resourcePartition, int upsertIdx,
            boolean isSink) {
        super(ctx, jobId, datasetId, primaryKeyFields, isTemporaryDatasetWriteJob, isWriteTransaction,
                resourcePartition, isSink);
        this.upsertIdx = upsertIdx;
    }

    @Override
    protected void formLogRecord(ByteBuffer buffer, int t) {
        boolean isNull = ABooleanSerializerDeserializer.getBoolean(buffer.array(), tAccess.getFieldSlotsLength()
                + tAccess.getTupleStartOffset(t) + tAccess.getFieldStartOffset(t, upsertIdx) + 1);
        if (isNull) {
            // Previous record not found (insert)
            super.formLogRecord(buffer, t);
        } else {
            // Previous record found (delete + insert)
            int pkHash = computePrimaryKeyHashValue(tRef, primaryKeyFields);
            TransactionUtil.formEntityCommitLogRecord(logRecord, transactionContext, datasetId, pkHash, tRef,
                    primaryKeyFields, resourcePartition, LogType.UPSERT_ENTITY_COMMIT);
        }
    }
}
