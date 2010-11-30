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
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrameFactory;

public class BTreeSearchOperatorDescriptor extends AbstractBTreeOperatorDescriptor {
	
	private static final long serialVersionUID = 1L;

	private boolean isForward;	
	private int[] lowKeyFields; // fields in input tuple to be used as low keys
	private int[] highKeyFields; // fields in input tuple to be used as high keys
	private boolean lowKeyInclusive;
    private boolean highKeyInclusive;
	
	public BTreeSearchOperatorDescriptor(JobSpecification spec, RecordDescriptor recDesc, IBufferCacheProvider bufferCacheProvider, IBTreeRegistryProvider btreeRegistryProvider, IFileSplitProvider fileSplitProvider, IFileMappingProviderProvider fileMappingProviderProvider, IBTreeInteriorFrameFactory interiorFactory, IBTreeLeafFrameFactory leafFactory, ITypeTrait[] typeTraits, IBinaryComparatorFactory[] comparatorFactories, boolean isForward, int[] lowKeyFields, int[] highKeyFields, boolean lowKeyInclusive, boolean highKeyInclusive) {		
		super(spec, 1, 1, recDesc, bufferCacheProvider, btreeRegistryProvider, fileSplitProvider, fileMappingProviderProvider, interiorFactory, leafFactory, typeTraits, comparatorFactories);
		this.isForward = isForward;
		this.lowKeyFields = lowKeyFields;
		this.highKeyFields = highKeyFields;
		this.lowKeyInclusive = lowKeyInclusive;
		this.highKeyInclusive = highKeyInclusive;
	}
	
	@Override
	public IOperatorNodePushable createPushRuntime(final IHyracksContext ctx, final IOperatorEnvironment env,
			IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
		return new BTreeSearchOperatorNodePushable(this, ctx, partition, recordDescProvider, isForward, lowKeyFields, highKeyFields, lowKeyInclusive, highKeyInclusive);
	}
}
