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

package org.apache.hyracks.storage.am.btree.dataflow;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.btree.exceptions.BTreeException;
import org.apache.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import org.apache.hyracks.storage.am.btree.util.BTreeUtils;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.dataflow.AbstractTreeIndexOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.IIndexOperatorDescriptor;
import org.apache.hyracks.storage.am.common.dataflow.TreeIndexDataflowHelper;

public class BTreeDataflowHelper extends TreeIndexDataflowHelper {

    public BTreeDataflowHelper(IIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx, int partition,
            boolean durable) {
        super(opDesc, ctx, partition, durable);
    }

    @Override
    public ITreeIndex createIndexInstance() throws HyracksDataException {
        AbstractTreeIndexOperatorDescriptor treeOpDesc = (AbstractTreeIndexOperatorDescriptor) opDesc;
        try {
            return BTreeUtils.createBTree(opDesc.getStorageManager().getBufferCache(ctx), opDesc.getStorageManager()
                    .getFileMapProvider(ctx), treeOpDesc.getTreeIndexTypeTraits(), treeOpDesc
                    .getTreeIndexComparatorFactories(), BTreeLeafFrameType.REGULAR_NSM, file);
        } catch (BTreeException e) {
            throw new HyracksDataException(e);
        }
    }
}
