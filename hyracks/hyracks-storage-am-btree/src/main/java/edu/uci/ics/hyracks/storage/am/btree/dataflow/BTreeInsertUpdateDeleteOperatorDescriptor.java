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

import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.api.IFieldAccessorFactory;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeOp;

public class BTreeInsertUpdateDeleteOperatorDescriptor extends AbstractBTreeOperatorDescriptor {
	
	private static final long serialVersionUID = 1L;
	
	private final int[] keyFields;
	private final int[] payloadFields;
	
	private BTreeOp op;
	
	public BTreeInsertUpdateDeleteOperatorDescriptor(JobSpecification spec,
			IFileSplitProvider fileSplitProvider, RecordDescriptor recDesc,
			IBufferCacheProvider bufferCacheProvider,
			IBTreeRegistryProvider btreeRegistryProvider, int btreeFileId,
			String btreeFileName, IBTreeInteriorFrameFactory interiorFactory,
			IBTreeLeafFrameFactory leafFactory, IFieldAccessorFactory[] fieldAccessorFactories, 
			IBinaryComparatorFactory[] comparatorFactories,			
			int[] keyFields, int[] payloadFields, BTreeOp op) {
		super(spec, 1, 1, fileSplitProvider, recDesc, bufferCacheProvider,
				btreeRegistryProvider, btreeFileId, btreeFileName, interiorFactory,
				leafFactory, fieldAccessorFactories, comparatorFactories);
		this.keyFields = keyFields;
		this.payloadFields = payloadFields;
		this.op = op;
	}
	
	@Override
	public IOperatorNodePushable createPushRuntime(IHyracksContext ctx,
			IOperatorEnvironment env,
			IRecordDescriptorProvider recordDescProvider, int partition,
			int nPartitions) {
		return new BTreeInsertUpdateDeleteOperatorNodePushable(this, ctx, keyFields, payloadFields, recordDescProvider, op);
	}
	
}
