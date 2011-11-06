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

package edu.uci.ics.hyracks.storage.am.common.dataflow;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;

public class TreeIndexInsertUpdateDeleteOperatorDescriptor extends
		AbstractTreeIndexOperatorDescriptor {

	private static final long serialVersionUID = 1L;

	private final int[] fieldPermutation;

	private IndexOp op;

	public TreeIndexInsertUpdateDeleteOperatorDescriptor(JobSpecification spec,
			RecordDescriptor recDesc, IStorageManagerInterface storageManager,
			IIndexRegistryProvider<ITreeIndex> treeIndexRegistryProvider,
			IFileSplitProvider fileSplitProvider,
			ITreeIndexFrameFactory interiorFrameFactory,
			ITreeIndexFrameFactory leafFrameFactory, ITypeTrait[] typeTraits,
			IBinaryComparatorFactory[] comparatorFactories,
			int[] fieldPermutation, IndexOp op,
			ITreeIndexOpHelperFactory opHelperFactory) {
		super(spec, 1, 1, recDesc, storageManager, treeIndexRegistryProvider,
				fileSplitProvider, interiorFrameFactory, leafFrameFactory,
				typeTraits, comparatorFactories,
				opHelperFactory);
		this.fieldPermutation = fieldPermutation;
		this.op = op;
	}

	@Override
	public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
			IRecordDescriptorProvider recordDescProvider,
			int partition, int nPartitions) {
		return new TreeIndexInsertUpdateDeleteOperatorNodePushable(this, ctx,
				partition, fieldPermutation, recordDescProvider, op);
	}
}