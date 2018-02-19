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
package org.apache.hyracks.storage.am.lsm.btree.dataflow;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.common.api.ISearchOperationCallbackFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.dataflow.IndexSearchOperatorNodePushable;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.common.IIndexAccessParameters;
import org.apache.hyracks.storage.common.ISearchPredicate;

public class LSMBTreeDiskComponentScanOperatorNodePushable extends IndexSearchOperatorNodePushable {

    public LSMBTreeDiskComponentScanOperatorNodePushable(IHyracksTaskContext ctx, int partition,
            RecordDescriptor inputRecDesc, IIndexDataflowHelperFactory indexHelperFactory,
            ISearchOperationCallbackFactory searchCallbackFactory) throws HyracksDataException {
        super(ctx, inputRecDesc, partition, null, null, indexHelperFactory, false, false, null, searchCallbackFactory,
                false);
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        try {
            ((ILSMIndexAccessor) indexAccessor).scanDiskComponents(cursor);
            writeSearchResults(0);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    protected ISearchPredicate createSearchPredicate() {
        // do nothing
        // no need to create search predicate for disk component scan operation
        return null;
    }

    @Override
    protected void resetSearchPredicate(int tupleIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected int getFieldCount() {
        return ((ITreeIndex) index).getFieldCount() + 2;
    }

    @Override
    protected void addAdditionalIndexAccessorParams(IIndexAccessParameters iap) throws HyracksDataException {
        // no additional parameters are required for the B+Tree search case
    }

}
