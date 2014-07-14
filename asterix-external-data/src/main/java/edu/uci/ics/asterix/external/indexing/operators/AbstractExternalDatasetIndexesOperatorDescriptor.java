/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.asterix.external.indexing.operators;

import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.lsm.btree.dataflow.ExternalBTreeDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.lsm.btree.dataflow.ExternalBTreeWithBuddyDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.dataflow.ExternalRTreeDataflowHelperFactory;

// This is an operator that takes a single file index and an array of secondary indexes
// it is intended to be used for 
// 1. commit transaction operation
// 2. abort transaction operation
// 3. recover transaction operation
public abstract class AbstractExternalDatasetIndexesOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private ExternalBTreeDataflowHelperFactory filesIndexDataflowHelperFactory;
    private IndexInfoOperatorDescriptor fileIndexInfo;
    private List<ExternalBTreeWithBuddyDataflowHelperFactory> bTreeIndexesDataflowHelperFactories;
    private List<IndexInfoOperatorDescriptor> bTreeIndexesInfos;
    private List<ExternalRTreeDataflowHelperFactory> rTreeIndexesDataflowHelperFactories;
    private List<IndexInfoOperatorDescriptor> rTreeIndexesInfos;

    public AbstractExternalDatasetIndexesOperatorDescriptor(IOperatorDescriptorRegistry spec,
            ExternalBTreeDataflowHelperFactory filesIndexDataflowHelperFactory,
            IndexInfoOperatorDescriptor fileIndexesInfo,
            List<ExternalBTreeWithBuddyDataflowHelperFactory> bTreeIndexesDataflowHelperFactories,
            List<IndexInfoOperatorDescriptor> bTreeIndexesInfos,
            List<ExternalRTreeDataflowHelperFactory> rTreeIndexesDataflowHelperFactories,
            List<IndexInfoOperatorDescriptor> rTreeIndexesInfos) {
        super(spec, 0, 0);
        this.filesIndexDataflowHelperFactory = filesIndexDataflowHelperFactory;
        this.fileIndexInfo = fileIndexesInfo;
        this.bTreeIndexesDataflowHelperFactories = bTreeIndexesDataflowHelperFactories;
        this.bTreeIndexesInfos = bTreeIndexesInfos;
        this.rTreeIndexesDataflowHelperFactories = rTreeIndexesDataflowHelperFactories;
        this.rTreeIndexesInfos = rTreeIndexesInfos;
    }

    // opening and closing the index is done inside these methods since we don't always need open indexes
    protected abstract void performOpOnIndex(
            IIndexDataflowHelperFactory indexDataflowHelperFactory, IHyracksTaskContext ctx,
            IndexInfoOperatorDescriptor fileIndexInfo, int partition) throws Exception;

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
        return new AbstractOperatorNodePushable() {

            @Override
            public void initialize() throws HyracksDataException {
                try {
                    // only in partition of device id = 0, we perform the operation on the files index
                    if(fileIndexInfo.getFileSplitProvider().getFileSplits()[partition].getIODeviceId() == 0){
                        performOpOnIndex(filesIndexDataflowHelperFactory, ctx, fileIndexInfo, partition);
                    }
                    // perform operation on btrees
                    for (int i = 0; i < bTreeIndexesDataflowHelperFactories.size(); i++) {
                        performOpOnIndex(bTreeIndexesDataflowHelperFactories.get(i), ctx,
                                bTreeIndexesInfos.get(i), partition);
                    }
                    // perform operation on rtrees
                    for (int i = 0; i < rTreeIndexesDataflowHelperFactories.size(); i++) {
                        performOpOnIndex(rTreeIndexesDataflowHelperFactories.get(i), ctx,
                                rTreeIndexesInfos.get(i), partition);
                    }
                } catch (Exception e) {
                    // This should never happen <unless there is a hardware failure or something serious>
                    throw new HyracksDataException(e);
                }
            }

            @Override
            public void deinitialize() throws HyracksDataException {
            }

            @Override
            public int getInputArity() {
                return 0;
            }

            @Override
            public void setOutputFrameWriter(int index, IFrameWriter writer, RecordDescriptor recordDesc)
                    throws HyracksDataException {
            }

            @Override
            public IFrameWriter getInputFrameWriter(int index) {
                return null;
            }

        };
    }
}
