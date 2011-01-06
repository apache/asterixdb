/*
 * Copyright 2009-2010 by The Regents of the University of California
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

package edu.uci.ics.hyracks.storage.am.btree.dataflow;

import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeOp;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;

public class BTreeInsertUpdateDeleteOperatorDescriptor extends AbstractBTreeOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private final int[] fieldPermutation;

    private BTreeOp op;

    public BTreeInsertUpdateDeleteOperatorDescriptor(JobSpecification spec, RecordDescriptor recDesc,
            IStorageManagerInterface storageManager, IBTreeRegistryProvider btreeRegistryProvider,
            IFileSplitProvider fileSplitProvider, IBTreeInteriorFrameFactory interiorFactory,
            IBTreeLeafFrameFactory leafFactory, ITypeTrait[] typeTraits,
            IBinaryComparatorFactory[] comparatorFactories, int[] fieldPermutation, BTreeOp op) {
        super(spec, 1, 1, recDesc, storageManager, btreeRegistryProvider, fileSplitProvider, interiorFactory,
                leafFactory, typeTraits, comparatorFactories);
        this.fieldPermutation = fieldPermutation;
        this.op = op;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksStageletContext ctx, IOperatorEnvironment env,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        return new BTreeInsertUpdateDeleteOperatorNodePushable(this, ctx, partition, fieldPermutation,
                recordDescProvider, op);
    }
}
