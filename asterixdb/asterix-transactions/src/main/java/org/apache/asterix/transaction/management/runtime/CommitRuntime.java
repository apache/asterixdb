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

package org.apache.asterix.transaction.management.runtime;

import java.nio.ByteBuffer;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.ILogManager;
import org.apache.asterix.common.transactions.ILogMarkerCallback;
import org.apache.asterix.common.transactions.ITransactionContext;
import org.apache.asterix.common.transactions.ITransactionManager;
import org.apache.asterix.common.transactions.LogRecord;
import org.apache.asterix.common.transactions.LogType;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.common.utils.TransactionUtil;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.io.MessagingFrameTupleAppender;
import org.apache.hyracks.dataflow.common.utils.TaskUtil;
import org.apache.hyracks.storage.am.bloomfilter.impls.MurmurHash128Bit;

public class CommitRuntime extends AbstractOneInputOneOutputOneFramePushRuntime {

    protected static final long SEED = 0L;

    protected final ITransactionManager transactionManager;
    protected final ILogManager logMgr;
    protected final TxnId txnId;
    protected final int datasetId;
    protected final int[] primaryKeyFields;
    protected final boolean isWriteTransaction;
    protected final long[] longHashes;
    protected final IHyracksTaskContext ctx;
    protected final int resourcePartition;
    protected ITransactionContext transactionContext;
    protected LogRecord logRecord;
    protected final boolean isSink;

    public CommitRuntime(IHyracksTaskContext ctx, TxnId txnId, int datasetId, int[] primaryKeyFields,
            boolean isWriteTransaction, int resourcePartition, boolean isSink) {
        this.ctx = ctx;
        INcApplicationContext appCtx =
                (INcApplicationContext) ctx.getJobletContext().getServiceContext().getApplicationContext();
        this.transactionManager = appCtx.getTransactionSubsystem().getTransactionManager();
        this.logMgr = appCtx.getTransactionSubsystem().getLogManager();
        this.txnId = txnId;
        this.datasetId = datasetId;
        this.primaryKeyFields = primaryKeyFields;
        this.tRef = new FrameTupleReference();
        this.isWriteTransaction = isWriteTransaction;
        this.resourcePartition = resourcePartition;
        this.isSink = isSink;
        longHashes = new long[2];
    }

    @Override
    public void open() throws HyracksDataException {
        try {
            transactionContext = transactionManager.getTransactionContext(txnId);
            transactionContext.setWriteTxn(isWriteTransaction);
            ILogMarkerCallback callback = TaskUtil.get(ILogMarkerCallback.KEY_MARKER_CALLBACK, ctx);
            logRecord = new LogRecord(callback);
            if (isSink) {
                return;
            }
            initAccessAppend(ctx);
            super.open();
        } catch (ACIDException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        tAccess.reset(buffer);
        int nTuple = tAccess.getTupleCount();
        for (int t = 0; t < nTuple; t++) {
            tRef.reset(tAccess, t);
            try {
                formLogRecord(buffer, t);
                logMgr.log(logRecord);
                if (!isSink) {
                    appendTupleToFrame(t);
                }
            } catch (ACIDException e) {
                throw HyracksDataException.create(e);
            }
        }
        IFrame message = TaskUtil.get(HyracksConstants.KEY_MESSAGE, ctx);
        if (message != null
                && MessagingFrameTupleAppender.getMessageType(message) == MessagingFrameTupleAppender.MARKER_MESSAGE) {
            try {
                formMarkerLogRecords(message.getBuffer());
                logMgr.log(logRecord);
            } catch (ACIDException e) {
                throw HyracksDataException.create(e);
            }
            message.reset();
            message.getBuffer().put(MessagingFrameTupleAppender.NULL_FEED_MESSAGE);
            message.getBuffer().flip();
        }
    }

    private void formMarkerLogRecords(ByteBuffer marker) {
        TransactionUtil.formMarkerLogRecord(logRecord, transactionContext, datasetId, resourcePartition, marker);
    }

    protected void formLogRecord(ByteBuffer buffer, int t) {
        int pkHash = computePrimaryKeyHashValue(tRef, primaryKeyFields);
        TransactionUtil.formEntityCommitLogRecord(logRecord, transactionContext, datasetId, pkHash, tRef,
                primaryKeyFields, resourcePartition, LogType.ENTITY_COMMIT);
    }

    protected int computePrimaryKeyHashValue(ITupleReference tuple, int[] primaryKeyFields) {
        MurmurHash128Bit.hash3_x64_128(tuple, primaryKeyFields, SEED, longHashes);
        return Math.abs((int) longHashes[0]);
    }

    @Override
    public void setInputRecordDescriptor(int index, RecordDescriptor recordDescriptor) {
        this.inputRecordDesc = recordDescriptor;
        this.tAccess = new FrameTupleAccessor(inputRecordDesc);
    }

    @Override
    public void flush() throws HyracksDataException {
        // Commit is at the end of a modification pipeline and there is no need to flush
    }
}
