/*
 * Copyright 2009-2012 by The Regents of the University of California
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

package edu.uci.ics.asterix.algebra.operators.physical;

import java.nio.ByteBuffer;

import edu.uci.ics.asterix.common.context.AsterixAppRuntimeContext;
import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.service.transaction.DatasetId;
import edu.uci.ics.asterix.transaction.management.service.transaction.ITransactionManager;
import edu.uci.ics.asterix.transaction.management.service.transaction.JobId;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext.TransactionType;
import edu.uci.ics.hyracks.algebricks.runtime.base.IPushRuntime;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.bloomfilter.impls.MurmurHash128Bit;

public class CommitRuntime implements IPushRuntime {
    
    private final static long SEED = 0L;

    private final IHyracksTaskContext hyracksTaskCtx;
    private final ITransactionManager transactionManager;
    private final JobId jobId;
    private final DatasetId datasetId;
    private final int[] primaryKeyFields;
    private final boolean isWriteTransaction;
    private final long[] longHashes; 

    private TransactionContext transactionContext;
    private RecordDescriptor inputRecordDesc;
    private FrameTupleAccessor frameTupleAccessor;
    private FrameTupleReference frameTupleReference;

    public CommitRuntime(IHyracksTaskContext ctx, JobId jobId, int datasetId, int[] primaryKeyFields,
            boolean isWriteTransaction) {
        this.hyracksTaskCtx = ctx;
        AsterixAppRuntimeContext runtimeCtx = (AsterixAppRuntimeContext) ctx.getJobletContext().getApplicationContext()
                .getApplicationObject();
        this.transactionManager = runtimeCtx.getTransactionSubsystem().getTransactionManager();
        this.jobId = jobId;
        this.datasetId = new DatasetId(datasetId);
        this.primaryKeyFields = primaryKeyFields;
        this.frameTupleReference = new FrameTupleReference();
        this.isWriteTransaction = isWriteTransaction;
        this.longHashes= new long[2];
    }

    @Override
    public void open() throws HyracksDataException {
        try {
            transactionContext = transactionManager.getTransactionContext(jobId);
            transactionContext.setTransactionType(isWriteTransaction ? TransactionType.READ_WRITE
                    : TransactionType.READ);
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
            frameTupleReference.reset(frameTupleAccessor, t);
            pkHash = computePrimaryKeyHashValue(frameTupleReference, primaryKeyFields);
            try {
                transactionManager.commitTransaction(transactionContext, datasetId, pkHash);
            } catch (ACIDException e) {
                throw new HyracksDataException(e);
            }
        }
    }
    
    private int computePrimaryKeyHashValue(ITupleReference tuple, int[] primaryKeyFields) {
        MurmurHash128Bit.hash3_x64_128(tuple, primaryKeyFields, SEED, longHashes);
        return Math.abs((int) longHashes[0]); 
    }

    @Override
    public void fail() throws HyracksDataException {
        // TODO Auto-generated method stub

    }

    @Override
    public void close() throws HyracksDataException {
        // TODO Auto-generated method stub
    }

    @Override
    public void setFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc) {
        throw new IllegalStateException();
    }

    @Override
    public void setInputRecordDescriptor(int index, RecordDescriptor recordDescriptor) {
        this.inputRecordDesc = recordDescriptor;
        this.frameTupleAccessor = new FrameTupleAccessor(hyracksTaskCtx.getFrameSize(), recordDescriptor);
    }
}
