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

import org.apache.asterix.common.api.IAsterixAppRuntimeContext;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.ILogManager;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionManager;
import org.apache.asterix.common.transactions.JobId;
import org.apache.asterix.common.transactions.LogRecord;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntime;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.bloomfilter.impls.MurmurHash128Bit;

public class CommitRuntime implements IPushRuntime {

    private final static long SEED = 0L;

    private final ITransactionManager transactionManager;
    private final ILogManager logMgr;
    private final JobId jobId;
    private final int datasetId;
    private final int[] primaryKeyFields;
    private final boolean isTemporaryDatasetWriteJob;
    private final boolean isWriteTransaction;
    private final long[] longHashes;
    private final LogRecord logRecord;

    private ITransactionContext transactionContext;
    private FrameTupleAccessor frameTupleAccessor;
    private final FrameTupleReference frameTupleReference;

    public CommitRuntime(IHyracksTaskContext ctx, JobId jobId, int datasetId, int[] primaryKeyFields,
            boolean isTemporaryDatasetWriteJob, boolean isWriteTransaction) {
        IAsterixAppRuntimeContext runtimeCtx = (IAsterixAppRuntimeContext) ctx.getJobletContext()
                .getApplicationContext().getApplicationObject();
        this.transactionManager = runtimeCtx.getTransactionSubsystem().getTransactionManager();
        this.logMgr = runtimeCtx.getTransactionSubsystem().getLogManager();
        this.jobId = jobId;
        this.datasetId = datasetId;
        this.primaryKeyFields = primaryKeyFields;
        this.frameTupleReference = new FrameTupleReference();
        this.isTemporaryDatasetWriteJob = isTemporaryDatasetWriteJob;
        this.isWriteTransaction = isWriteTransaction;
        this.longHashes = new long[2];
        this.logRecord = new LogRecord();
        logRecord.setNodeId(logMgr.getNodeId());
    }

    @Override
    public void open() throws HyracksDataException {
        try {
            transactionContext = transactionManager.getTransactionContext(jobId, false);
            transactionContext.setWriteTxn(isWriteTransaction);
        } catch (ACIDException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        int pkHash = 0;
        frameTupleAccessor.reset(buffer);
        int nTuple = frameTupleAccessor.getTupleCount();
        for (int t = 0; t < nTuple; t++) {
            if (isTemporaryDatasetWriteJob) {
                /**
                 * This "if branch" is for writes over temporary datasets.
                 * A temporary dataset does not require any lock and does not generate any write-ahead
                 * update and commit log but generates flush log and job commit log.
                 * However, a temporary dataset still MUST guarantee no-steal policy so that this
                 * notification call should be delivered to PrimaryIndexOptracker and used correctly in order
                 * to decrement number of active operation count of PrimaryIndexOptracker.
                 * By maintaining the count correctly and only allowing flushing when the count is 0, it can
                 * guarantee the no-steal policy for temporary datasets, too.
                 */
                transactionContext.notifyOptracker(false);
            } else {
                frameTupleReference.reset(frameTupleAccessor, t);
                pkHash = computePrimaryKeyHashValue(frameTupleReference, primaryKeyFields);
                logRecord.formEntityCommitLogRecord(transactionContext, datasetId, pkHash, frameTupleReference,
                        primaryKeyFields);
                try {
                    logMgr.log(logRecord);
                } catch (ACIDException e) {
                    throw new HyracksDataException(e);
                }
            }
        }
    }

    private int computePrimaryKeyHashValue(ITupleReference tuple, int[] primaryKeyFields) {
        MurmurHash128Bit.hash3_x64_128(tuple, primaryKeyFields, SEED, longHashes);
        return Math.abs((int) longHashes[0]);
    }

    @Override
    public void fail() throws HyracksDataException {

    }

    @Override
    public void close() throws HyracksDataException {

    }

    @Override
    public void setFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc) {
        throw new IllegalStateException();
    }

    @Override
    public void setInputRecordDescriptor(int index, RecordDescriptor recordDescriptor) {
        this.frameTupleAccessor = new FrameTupleAccessor(recordDescriptor);
    }
}