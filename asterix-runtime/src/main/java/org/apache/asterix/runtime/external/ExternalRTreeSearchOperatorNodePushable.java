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
package edu.uci.ics.asterix.runtime.external;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.hyracks.api.comm.VSizeFrame;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.dataflow.AbstractTreeIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.dataflow.ExternalRTreeDataflowHelper;
import edu.uci.ics.hyracks.storage.am.lsm.rtree.impls.ExternalRTree;
import edu.uci.ics.hyracks.storage.am.rtree.dataflow.RTreeSearchOperatorNodePushable;

public class ExternalRTreeSearchOperatorNodePushable extends RTreeSearchOperatorNodePushable {

    public ExternalRTreeSearchOperatorNodePushable(AbstractTreeIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx,
            int partition, IRecordDescriptorProvider recordDescProvider, int[] keyFields) {
        super(opDesc, ctx, partition, recordDescProvider, keyFields, null, null);
    }

    // We override this method to specify the searched version of the index
    @Override
    public void open() throws HyracksDataException {
        accessor = new FrameTupleAccessor(inputRecDesc);
        writer.open();
        indexHelper.open();
        ExternalRTreeDataflowHelper rTreeDataflowHelper = (ExternalRTreeDataflowHelper) indexHelper;
        index = indexHelper.getIndexInstance();

        if (retainNull) {
            int fieldCount = getFieldCount();
            nullTupleBuild = new ArrayTupleBuilder(fieldCount);
            DataOutput out = nullTupleBuild.getDataOutput();
            for (int i = 0; i < fieldCount; i++) {
                try {
                    nullWriter.writeNull(out);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                nullTupleBuild.addFieldEndOffset();
            }
        } else {
            nullTupleBuild = null;
        }

        ExternalRTree rTreeIndex = (ExternalRTree) index;
        try {
            searchPred = createSearchPredicate();
            tb = new ArrayTupleBuilder(recordDesc.getFieldCount());
            dos = tb.getDataOutput();
            appender = new FrameTupleAppender(new VSizeFrame(ctx));
            ISearchOperationCallback searchCallback = opDesc.getSearchOpCallbackFactory()
                    .createSearchOperationCallback(indexHelper.getResourceID(), ctx);
            // The next line is the reason we override this method
            indexAccessor = rTreeIndex.createAccessor(searchCallback, rTreeDataflowHelper.getTargetVersion());
            cursor = createCursor();
            if (retainInput) {
                frameTuple = new FrameTupleReference();
            }
        } catch (Exception e) {
            indexHelper.close();
            throw new HyracksDataException(e);
        }
    }

}
