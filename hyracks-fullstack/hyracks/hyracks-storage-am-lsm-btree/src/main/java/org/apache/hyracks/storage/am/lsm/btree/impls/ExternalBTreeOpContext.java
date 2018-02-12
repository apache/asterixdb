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
package org.apache.hyracks.storage.am.lsm.btree.impls;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexOperationContext;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.util.trace.ITracer;

public class ExternalBTreeOpContext extends AbstractLSMIndexOperationContext {
    private IBTreeLeafFrame insertLeafFrame;
    private IBTreeLeafFrame deleteLeafFrame;
    private final MultiComparator cmp;
    private final MultiComparator bloomFilterCmp;
    private final int targetIndexVersion;
    private LSMBTreeCursorInitialState searchInitialState;

    public ExternalBTreeOpContext(ILSMIndex index, ITreeIndexFrameFactory insertLeafFrameFactory,
            ITreeIndexFrameFactory deleteLeafFrameFactory, ISearchOperationCallback searchCallback,
            int numBloomFilterKeyFields, IBinaryComparatorFactory[] cmpFactories, int targetIndexVersion,
            ILSMHarness lsmHarness, ITracer tracer) {
        super(index, null, null, null, searchCallback, null, tracer);
        if (cmpFactories != null) {
            this.cmp = MultiComparator.create(cmpFactories);
        } else {
            this.cmp = null;
        }
        bloomFilterCmp = MultiComparator.create(cmpFactories, 0, numBloomFilterKeyFields);
        this.insertLeafFrame = (IBTreeLeafFrame) insertLeafFrameFactory.createFrame();
        this.deleteLeafFrame = (IBTreeLeafFrame) deleteLeafFrameFactory.createFrame();
        if (insertLeafFrame != null && this.cmp != null) {
            insertLeafFrame.setMultiComparator(cmp);
        }
        if (deleteLeafFrame != null && this.cmp != null) {
            deleteLeafFrame.setMultiComparator(cmp);
        }
        this.targetIndexVersion = targetIndexVersion;
        searchInitialState = new LSMBTreeCursorInitialState(insertLeafFrameFactory, cmp, bloomFilterCmp, lsmHarness,
                null, searchCallback, null);
    }

    @Override
    public void setCurrentMutableComponentId(int currentMutableComponentId) {
        // Do nothing: this method should never be called for this class
    }

    // Used by indexes with global transaction
    public int getTargetIndexVersion() {
        return targetIndexVersion;
    }

    public LSMBTreeCursorInitialState getSearchInitialState() {
        return searchInitialState;
    }

    @Override
    public void destroy() throws HyracksDataException {
        // No Op
    }
}
