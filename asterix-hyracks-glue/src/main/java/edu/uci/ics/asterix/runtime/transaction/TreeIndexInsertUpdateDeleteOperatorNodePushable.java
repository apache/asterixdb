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

import edu.uci.ics.asterix.common.context.AsterixAppRuntimeContext;
import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.resource.TransactionalResourceRepository;
import edu.uci.ics.asterix.transaction.management.service.locking.ILockManager;
import edu.uci.ics.asterix.transaction.management.service.logging.DataUtil;
import edu.uci.ics.asterix.transaction.management.service.logging.TreeLogger;
import edu.uci.ics.asterix.transaction.management.service.logging.TreeResourceManager;
import edu.uci.ics.asterix.transaction.management.service.transaction.DatasetId;
import edu.uci.ics.asterix.transaction.management.service.transaction.IResourceManager;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionManagementConstants;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionProvider;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexDataflowHelper;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITupleFilter;
import edu.uci.ics.hyracks.storage.am.common.api.ITupleFilterFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.AbstractTreeIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.am.common.tuples.PermutingFrameTupleReference;
import edu.uci.ics.hyracks.storage.common.file.IIndexArtifactMap;

public class TreeIndexInsertUpdateDeleteOperatorNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable {
    private final AbstractTreeIndexOperatorDescriptor opDesc;
    private final IHyracksTaskContext ctx;
    private final IIndexDataflowHelper treeIndexHelper;
    private FrameTupleAccessor accessor;
    private final IRecordDescriptorProvider recordDescProvider;
    private final IndexOp op;
    private final PermutingFrameTupleReference tuple = new PermutingFrameTupleReference();
    private FrameTupleReference frameTuple;
    private ByteBuffer writeBuffer;
    private IIndexAccessor indexAccessor;
    private ITupleFilter tupleFilter;
    private IModificationOperationCallback modCallback;
    private ILockManager lockManager;
    private final TransactionContext txnContext;
    private TreeLogger treeLogger;
    private final TransactionProvider transactionProvider;
    private byte[] resourceIDBytes;
    private final DatasetId datasetId;

    /* TODO: Index operators should live in Hyracks. Right now, they are needed here in Asterix
     * as a hack to provide transactionIDs. The Asterix verions of this operator will disappear 
     * and the operator will come from Hyracks once the LSM/Recovery/Transactions world has 
     * been introduced.
     */
    public TreeIndexInsertUpdateDeleteOperatorNodePushable(TransactionContext txnContext,
            AbstractTreeIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx, int partition, int[] fieldPermutation,
            IRecordDescriptorProvider recordDescProvider, IndexOp op, int datasetId) {
        this.ctx = ctx;
        this.opDesc = opDesc;
        this.treeIndexHelper = opDesc.getIndexDataflowHelperFactory().createIndexDataflowHelper(opDesc, ctx, partition);
        this.recordDescProvider = recordDescProvider;
        this.op = op;
        tuple.setFieldPermutation(fieldPermutation);
        this.txnContext = txnContext;
        AsterixAppRuntimeContext runtimeContext = (AsterixAppRuntimeContext) ctx.getJobletContext()
                .getApplicationContext().getApplicationObject();
        transactionProvider = runtimeContext.getTransactionProvider();
        this.datasetId = new DatasetId(datasetId);
    }

    public void initializeTransactionSupport(long resourceID) {
        TransactionalResourceRepository resourceRepository = transactionProvider.getTransactionalResourceRepository();
        IResourceManager resourceMgr = resourceRepository.getTransactionalResourceMgr(TreeResourceManager.ID);
        if (resourceMgr == null) {
            resourceRepository.registerTransactionalResourceManager(TreeResourceManager.ID, new TreeResourceManager(
                    transactionProvider));
        }
        resourceIDBytes = DataUtil.longToByteArray(resourceID);
        transactionProvider.getTransactionalResourceRepository().registerTransactionalResource(resourceIDBytes,
                treeIndexHelper.getIndexInstance());
        lockManager = transactionProvider.getLockManager();
        treeLogger = transactionProvider.getTreeLoggerRepository().getTreeLogger(resourceIDBytes);
    }

    @Override
    public void open() throws HyracksDataException {
        RecordDescriptor inputRecDesc = recordDescProvider.getInputRecordDescriptor(opDesc.getActivityId(), 0);
        accessor = new FrameTupleAccessor(ctx.getFrameSize(), inputRecDesc);
        writeBuffer = ctx.allocateFrame();
        writer.open();
        treeIndexHelper.open();
        ITreeIndex treeIndex = (ITreeIndex) treeIndexHelper.getIndexInstance();
        try {
            modCallback = opDesc.getOpCallbackProvider().getModificationOperationCallback(
                    treeIndexHelper.getResourceID(), ctx);
            indexAccessor = treeIndex.createAccessor(modCallback, NoOpOperationCallback.INSTANCE);
            ITupleFilterFactory tupleFilterFactory = opDesc.getTupleFilterFactory();
            if (tupleFilterFactory != null) {
                tupleFilter = tupleFilterFactory.createTupleFilter(ctx);
                frameTuple = new FrameTupleReference();
            }
            IIndexArtifactMap iam = opDesc.getStorageManager().getIndexArtifactMap(ctx);
            long resourceID = iam.get(treeIndexHelper.getFileReference().getFile().getPath());
            initializeTransactionSupport(resourceID);
        } catch (Exception e) {
            treeIndexHelper.close();
            throw new HyracksDataException(e);
        }
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        accessor.reset(buffer);
        int tupleCount = accessor.getTupleCount();
        try {
            for (int i = 0; i < tupleCount; i++) {
                if (tupleFilter != null) {
                    frameTuple.reset(accessor, i);
                    if (!tupleFilter.accept(frameTuple)) {
                        continue;
                    }
                }
                tuple.reset(accessor, i);
                lockManager.lock(datasetId, -1, TransactionManagementConstants.LockManagerConstants.LockMode.X,
                        txnContext);
                switch (op) {
                    case INSERT: {
                        indexAccessor.insert(tuple);
                        break;
                    }
                    case UPDATE: {
                        indexAccessor.update(tuple);
                        break;
                    }
                    case UPSERT: {
                        indexAccessor.upsert(tuple);
                        break;
                    }
                    case DELETE: {
                        indexAccessor.delete(tuple);
                        break;
                    }
                    default: {
                        throw new HyracksDataException("Unsupported operation " + op
                                + " in tree index InsertUpdateDelete operator");
                    }
                }
                treeLogger.generateLogRecord(transactionProvider, txnContext, op, tuple);
            }
        } catch (ACIDException ae) {
            throw new HyracksDataException("exception in locking/logging during operation " + op, ae);
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
            treeIndexHelper.close();
            //            txnContext.addCloseableResource(new ICloseable() {
            //                @Override
            //                public void close(TransactionContext txnContext) throws ACIDException {
            //                    try {
            //                        treeIndexHelper.close();
            //                    } catch (Exception e) {
            //                        throw new ACIDException(txnContext, "could not de-initialize " + treeIndexHelper, e);
            //                    }
            //                }
            //            });
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.fail();
    }
}
