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

import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionerFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.dataflow.common.io.MessagingFrameTupleAppender;
import org.apache.hyracks.dataflow.common.utils.TaskUtil;

public class NoOpCommitRuntime extends CommitRuntime {

    public NoOpCommitRuntime(IHyracksTaskContext ctx, TxnId txnId, int datasetId, int[] primaryKeyFields,
            boolean isWriteTransaction, int resourcePartition, boolean isSink,
            ITuplePartitionerFactory partitionerFactory, int[] datasetPartitions) {
        super(ctx, txnId, datasetId, primaryKeyFields, isWriteTransaction, resourcePartition, isSink,
                partitionerFactory, datasetPartitions);
    }

    @Override
    public void open() throws HyracksDataException {
        try {
            transactionContext = transactionManager.getTransactionContext(txnId);
            transactionContext.setWriteTxn(isWriteTransaction);
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
            message.reset();
            message.getBuffer().put(MessagingFrameTupleAppender.NULL_FEED_MESSAGE);
            message.getBuffer().flip();
        }
    }

    @Override
    public void fail() {
        failed = true;
    }
}
