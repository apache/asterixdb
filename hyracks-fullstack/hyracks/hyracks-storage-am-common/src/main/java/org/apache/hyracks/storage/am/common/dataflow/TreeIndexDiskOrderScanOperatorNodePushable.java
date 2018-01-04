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
package org.apache.hyracks.storage.am.common.dataflow;

import java.io.DataOutput;

import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.api.ITreeIndexAccessor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.impls.TreeIndexDiskOrderScanCursor;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.LocalResource;

public class TreeIndexDiskOrderScanOperatorNodePushable extends AbstractUnaryOutputSourceOperatorNodePushable {
    private final IHyracksTaskContext ctx;
    private final IIndexDataflowHelper treeIndexHelper;
    private final ISearchOperationCallbackFactory searchCallbackFactory;

    public TreeIndexDiskOrderScanOperatorNodePushable(IHyracksTaskContext ctx, int partition,
            IIndexDataflowHelperFactory indexHelperFactory, ISearchOperationCallbackFactory searchCallbackFactory)
            throws HyracksDataException {
        this.ctx = ctx;
        this.treeIndexHelper = indexHelperFactory.create(ctx.getJobletContext().getServiceContext(), partition);
        this.searchCallbackFactory = searchCallbackFactory;
    }

    @Override
    public void initialize() throws HyracksDataException {
        treeIndexHelper.open();
        ITreeIndex treeIndex = (ITreeIndex) treeIndexHelper.getIndexInstance();
        try {
            ITreeIndexFrame cursorFrame = treeIndex.getLeafFrameFactory().createFrame();
            ITreeIndexCursor cursor = new TreeIndexDiskOrderScanCursor(cursorFrame);
            LocalResource resource = treeIndexHelper.getResource();
            ISearchOperationCallback searchCallback =
                    searchCallbackFactory.createSearchOperationCallback(resource.getId(), ctx, null);
            IIndexAccessParameters iap = new IndexAccessParameters(NoOpOperationCallback.INSTANCE, searchCallback);
            ITreeIndexAccessor indexAccessor = (ITreeIndexAccessor) treeIndex.createAccessor(iap);
            try {
                writer.open();
                indexAccessor.diskOrderScan(cursor);
                int fieldCount = treeIndex.getFieldCount();
                FrameTupleAppender appender = new FrameTupleAppender(new VSizeFrame(ctx));
                ArrayTupleBuilder tb = new ArrayTupleBuilder(fieldCount);
                DataOutput dos = tb.getDataOutput();

                while (cursor.hasNext()) {
                    tb.reset();
                    cursor.next();

                    ITupleReference frameTuple = cursor.getTuple();
                    for (int i = 0; i < frameTuple.getFieldCount(); i++) {
                        dos.write(frameTuple.getFieldData(i), frameTuple.getFieldStart(i),
                                frameTuple.getFieldLength(i));
                        tb.addFieldEndOffset();
                    }

                    FrameUtils.appendToWriter(writer, appender, tb.getFieldEndOffsets(), tb.getByteArray(), 0,
                            tb.getSize());

                }
                appender.write(writer, true);
            } catch (Throwable th) {
                writer.fail();
                throw new HyracksDataException(th);
            } finally {
                try {
                    cursor.destroy();
                } catch (Exception cursorCloseException) {
                    throw new IllegalStateException(cursorCloseException);
                } finally {
                    writer.close();
                }
            }
        } catch (Throwable th) {
            treeIndexHelper.close();
            throw new HyracksDataException(th);
        }
    }

    @Override
    public void deinitialize() throws HyracksDataException {
        treeIndexHelper.close();
    }
}
