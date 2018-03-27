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

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.util.TreeIndexStats;
import org.apache.hyracks.storage.am.common.util.TreeIndexStatsGatherer;
import org.apache.hyracks.storage.common.IStorageManager;
import org.apache.hyracks.storage.common.LocalResource;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;

public class TreeIndexStatsOperatorNodePushable extends AbstractUnaryOutputSourceOperatorNodePushable {
    private final IHyracksTaskContext ctx;
    private final IStorageManager storageManager;
    private final IIndexDataflowHelper treeIndexHelper;
    private final UTF8StringSerializerDeserializer utf8SerDer = new UTF8StringSerializerDeserializer();

    public TreeIndexStatsOperatorNodePushable(IHyracksTaskContext ctx, int partition,
            IIndexDataflowHelperFactory indexHelperFactory, IStorageManager storageManager)
            throws HyracksDataException {
        this.ctx = ctx;
        this.treeIndexHelper = indexHelperFactory.create(ctx.getJobletContext().getServiceContext(), partition);
        this.storageManager = storageManager;
    }

    @Override
    public void deinitialize() throws HyracksDataException {
    }

    @Override
    public IFrameWriter getInputFrameWriter(int index) {
        return null;
    }

    @Override
    public void initialize() throws HyracksDataException {
        treeIndexHelper.open();
        ITreeIndex treeIndex = (ITreeIndex) treeIndexHelper.getIndexInstance();
        try {
            writer.open();
            IBufferCache bufferCache = storageManager.getBufferCache(ctx.getJobletContext().getServiceContext());
            LocalResource resource = treeIndexHelper.getResource();
            IIOManager ioManager = ctx.getIoManager();
            FileReference fileRef = ioManager.resolve(resource.getPath());
            TreeIndexStatsGatherer statsGatherer = new TreeIndexStatsGatherer(bufferCache, treeIndex.getPageManager(),
                    fileRef, treeIndex.getRootPageId());
            TreeIndexStats stats = statsGatherer.gatherStats(treeIndex.getLeafFrameFactory().createFrame(),
                    treeIndex.getInteriorFrameFactory().createFrame(),
                    treeIndex.getPageManager().createMetadataFrame());
            // Write the stats output as a single string field.
            FrameTupleAppender appender = new FrameTupleAppender(new VSizeFrame(ctx));
            ArrayTupleBuilder tb = new ArrayTupleBuilder(1);
            DataOutput dos = tb.getDataOutput();
            tb.reset();
            utf8SerDer.serialize(stats.toString(), dos);
            tb.addFieldEndOffset();
            if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                throw new HyracksDataException("Record size (" + tb.getSize() + ") larger than frame size ("
                        + appender.getBuffer().capacity() + ")");
            }
            appender.write(writer, false);
        } catch (Exception e) {
            writer.fail();
            throw HyracksDataException.create(e);
        } finally {
            try {
                writer.close();
            } finally {
                treeIndexHelper.close();
            }
        }
    }
}
