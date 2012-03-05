/*
 * Copyright 2009-2011 by The Regents of the University of California
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
package edu.uci.ics.asterix.runtime.transaction;

import java.nio.ByteBuffer;

import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.resource.ICloseable;
import edu.uci.ics.asterix.transaction.management.resource.TransactionalResourceRepository;
import edu.uci.ics.asterix.transaction.management.service.locking.ILockManager;
import edu.uci.ics.asterix.transaction.management.service.logging.DataUtil;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionManagementConstants;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionProvider;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.AbstractTreeIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;
import edu.uci.ics.hyracks.storage.am.common.dataflow.PermutingFrameTupleReference;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexDataflowHelper;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;

public class TreeIndexInsertUpdateDeleteOperatorNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {
    private FrameTupleAccessor accessor;
    private TreeIndexDataflowHelper treeIndexHelper;
    private final IRecordDescriptorProvider recordDescProvider;
    private final IndexOp op;
    private final PermutingFrameTupleReference tuple = new PermutingFrameTupleReference();
    private ByteBuffer writeBuffer;
    private ILockManager lockManager;
    private final TransactionContext txnContext;
    private TreeLogger bTreeLogger;
    private final TransactionProvider transactionProvider;
    private ITreeIndexAccessor treeIndexAccessor;

    public TreeIndexInsertUpdateDeleteOperatorNodePushable(TransactionContext txnContext,
            AbstractTreeIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx, int partition, int[] fieldPermutation,
            IRecordDescriptorProvider recordDescProvider, IndexOp op) {
        treeIndexHelper = (TreeIndexDataflowHelper) opDesc.getIndexDataflowHelperFactory().createIndexDataflowHelper(
                opDesc, ctx, partition, false);
        this.recordDescProvider = recordDescProvider;
        this.op = op;
        tuple.setFieldPermutation(fieldPermutation);
        this.txnContext = txnContext;
        transactionProvider = (TransactionProvider) ctx.getJobletContext().getApplicationContext()
                .getApplicationObject();
    }

    public void initializeTransactionSupport() {
        TransactionalResourceRepository.registerTransactionalResourceManager(TreeResourceManager.ID,
                TreeResourceManager.getInstance());
        int fileId = treeIndexHelper.getIndexFileId();
        byte[] resourceId = DataUtil.intToByteArray(fileId);
        TransactionalResourceRepository.registerTransactionalResource(resourceId, treeIndexHelper.getIndex());
        lockManager = transactionProvider.getLockManager();
        bTreeLogger = TreeLoggerRepository.getTreeLogger(resourceId);
    }

    @Override
    public void open() throws HyracksDataException {
        AbstractTreeIndexOperatorDescriptor opDesc = (AbstractTreeIndexOperatorDescriptor) treeIndexHelper
                .getOperatorDescriptor();
        RecordDescriptor inputRecDesc = recordDescProvider.getInputRecordDescriptor(opDesc.getOperatorId(), 0);
        accessor = new FrameTupleAccessor(treeIndexHelper.getHyracksTaskContext().getFrameSize(), inputRecDesc);
        writeBuffer = treeIndexHelper.getHyracksTaskContext().allocateFrame();
        writer.open();
        try {
            treeIndexHelper.init();
            treeIndexHelper.getIndex().open(treeIndexHelper.getIndexFileId());
            treeIndexAccessor = ((ITreeIndex) treeIndexHelper.getIndex()).createAccessor();
            initializeTransactionSupport();
        } catch (Exception e) {
            // cleanup in case of failure
            treeIndexHelper.deinit();
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        final IIndex treeIndex = treeIndexHelper.getIndex();
        accessor.reset(buffer);
        int fileId = treeIndexHelper.getIndexFileId();
        byte[] resourceId = DataUtil.intToByteArray(fileId);
        int tupleCount = accessor.getTupleCount();
        try {
            for (int i = 0; i < tupleCount; i++) {
                tuple.reset(accessor, i);
                switch (op) {
                    case INSERT: {
                        lockManager.lock(txnContext, resourceId,
                                TransactionManagementConstants.LockManagerConstants.LockMode.EXCLUSIVE);
                        treeIndexAccessor.insert(tuple);
                        bTreeLogger.generateLogRecord(transactionProvider, txnContext, op, tuple);
                    }
                        break;

                    case DELETE: {
                        lockManager.lock(txnContext, resourceId,
                                TransactionManagementConstants.LockManagerConstants.LockMode.EXCLUSIVE);
                        treeIndexAccessor.delete(tuple);
                        bTreeLogger.generateLogRecord(transactionProvider, txnContext, op, tuple);
                    }
                        break;

                    default: {
                        throw new HyracksDataException("Unsupported operation " + op
                                + " in tree index InsertUpdateDelete operator");
                    }
                }
            }
        } catch (ACIDException ae) {
            throw new HyracksDataException("exception in locking/logging during operation " + op + " on tree "
                    + treeIndex, ae);
        } catch (Exception e) {
            e.printStackTrace();
            throw new HyracksDataException(e);
        }

        // pass a copy of the frame to next op
        System.arraycopy(buffer.array(), 0, writeBuffer.array(), 0, buffer.capacity());
        FrameUtils.flushFrame(writeBuffer, writer);

    }

    @Override
    public void close() throws HyracksDataException {
        try {
            writer.close();
        } finally {
            txnContext.addCloseableResource(new ICloseable() {
                @Override
                public void close(TransactionContext txnContext) throws ACIDException {
                    try {
                        treeIndexHelper.deinit();
                    } catch (Exception e) {
                        throw new ACIDException(txnContext, "could not de-initialize " + treeIndexHelper, e);
                    }
                }
            });
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }

}
