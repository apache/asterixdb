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
package org.apache.asterix.external.operators;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IMissingWriterFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.storage.am.btree.dataflow.BTreeSearchOperatorNodePushable;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.lsm.btree.impls.ExternalBTreeWithBuddy;
import org.apache.hyracks.storage.common.ISearchOperationCallback;

public class ExternalBTreeSearchOperatorNodePushable extends BTreeSearchOperatorNodePushable {

    private final int version;

    public ExternalBTreeSearchOperatorNodePushable(IHyracksTaskContext ctx, int partition,
            RecordDescriptor intputRecDesc, int[] lowKeyFields, int[] highKeyFields, boolean lowKeyInclusive,
            boolean highKeyInclusive, IIndexDataflowHelperFactory indexHelperFactory, boolean retainInput,
            boolean retainMissing, IMissingWriterFactory missingWriterFactory,
            ISearchOperationCallbackFactory searchCallbackFactory, int version) throws HyracksDataException {
        super(ctx, partition, intputRecDesc, lowKeyFields, highKeyFields, lowKeyInclusive, highKeyInclusive, null, null,
                indexHelperFactory, retainInput, retainMissing, missingWriterFactory, searchCallbackFactory, false);
        this.version = version;
    }

    // We override the open function to search a specific version of the index
    @Override
    public void open() throws HyracksDataException {
        writer.open();
        accessor = new FrameTupleAccessor(inputRecDesc);
        indexHelper.open();
        index = indexHelper.getIndexInstance();
        if (retainMissing) {
            int fieldCount = getFieldCount();
            nonMatchTupleBuild = new ArrayTupleBuilder(fieldCount);
            DataOutput out = nonMatchTupleBuild.getDataOutput();
            for (int i = 0; i < fieldCount; i++) {
                try {
                    nonMatchWriter.writeMissing(out);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                nonMatchTupleBuild.addFieldEndOffset();
            }
        } else {
            nonMatchTupleBuild = null;
        }
        ExternalBTreeWithBuddy externalIndex = (ExternalBTreeWithBuddy) index;
        try {
            searchPred = createSearchPredicate();
            tb = new ArrayTupleBuilder(recordDesc.getFieldCount());
            dos = tb.getDataOutput();
            appender = new FrameTupleAppender(new VSizeFrame(ctx));
            ISearchOperationCallback searchCallback =
                    searchCallbackFactory.createSearchOperationCallback(indexHelper.getResource().getId(), ctx, null);
            // The next line is the reason we override this method
            indexAccessor = externalIndex.createAccessor(searchCallback, version);
            cursor = createCursor();
            if (retainInput) {
                frameTuple = new FrameTupleReference();
            }
        } catch (Throwable th) {
            throw HyracksDataException.create(th);
        }
    }
}
