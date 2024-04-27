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
package org.apache.hyracks.storage.am.lsm.btree.column.impls.lsm;

import java.util.List;

import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.storage.am.common.api.IExtendedModificationOperationCallback;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnProjectionInfo;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnTupleProjector;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.IColumnReadContext;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.lsm.tuples.ColumnAwareMultiComparator;
import org.apache.hyracks.storage.am.lsm.btree.column.utils.ColumnUtil;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTreeOpContext;
import org.apache.hyracks.storage.am.lsm.common.api.IComponentMetadata;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent.LSMComponentType;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndex;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMMemoryComponent;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.util.trace.ITracer;

public class LSMColumnBTreeOpContext extends LSMBTreeOpContext {
    private final IColumnTupleProjector projector;

    public LSMColumnBTreeOpContext(ILSMIndex index, List<ILSMMemoryComponent> mutableComponents,
            ITreeIndexFrameFactory insertLeafFrameFactory, ITreeIndexFrameFactory deleteLeafFrameFactory,
            IExtendedModificationOperationCallback modificationCallback, ISearchOperationCallback searchCallback,
            int numBloomFilterKeyFields, int[] btreeFields, int[] filterFields, ILSMHarness lsmHarness,
            IBinaryComparatorFactory[] filterCmpFactories, ITracer tracer, IColumnTupleProjector projector) {
        super(index, mutableComponents, insertLeafFrameFactory, deleteLeafFrameFactory, modificationCallback,
                searchCallback, numBloomFilterKeyFields, btreeFields, filterFields, lsmHarness, filterCmpFactories,
                tracer);
        this.projector = projector;
    }

    public IColumnProjectionInfo createProjectionInfo() throws HyracksDataException {
        List<ILSMComponent> operationalComponents = getComponentHolder();
        IComponentMetadata componentMetadata = null;
        for (int i = 0; i < operationalComponents.size() && componentMetadata == null; i++) {
            ILSMComponent component = operationalComponents.get(i);
            if (component.getType() == LSMComponentType.DISK) {
                //Find the first on-disk component, which has the most recent column metadata.
                componentMetadata = component.getMetadata();
            }
        }
        if (componentMetadata != null) {
            IValueReference columnMetadata = ColumnUtil.getColumnMetadataCopy(componentMetadata);
            return projector.createProjectionInfo(columnMetadata);
        }
        //In-memory components only
        return null;
    }

    @Override
    protected MultiComparator createMultiComparator(IBinaryComparatorFactory[] cmpFactories) {
        IBinaryComparator[] comparators = new IBinaryComparator[cmpFactories.length];
        for (int i = 0; i < comparators.length; i++) {
            comparators[i] = cmpFactories[i].createBinaryComparator();
        }
        return new ColumnAwareMultiComparator(comparators);
    }

    public IColumnReadContext createPageZeroContext(IColumnProjectionInfo projectionInfo) {
        return ((LSMColumnBTree) index).getDiskCacheManager().createReadContext(projectionInfo);
    }
}
