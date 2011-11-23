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

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;

public abstract class AbstractTreeIndexOperatorDescriptor extends
		AbstractSingleActivityOperatorDescriptor implements
		ITreeIndexOperatorDescriptor {

	private static final long serialVersionUID = 1L;

	protected final IFileSplitProvider fileSplitProvider;

	protected final IBinaryComparatorFactory[] comparatorFactories;

	protected final ITreeIndexFrameFactory interiorFrameFactory;
	protected final ITreeIndexFrameFactory leafFrameFactory;

	protected final IStorageManagerInterface storageManager;
	protected final IIndexRegistryProvider<IIndex> indexRegistryProvider;

	protected final ITypeTrait[] typeTraits;
	protected final IIndexDataflowHelperFactory dataflowHelperFactory;

	public AbstractTreeIndexOperatorDescriptor(JobSpecification spec,
			int inputArity, int outputArity, RecordDescriptor recDesc,
			IStorageManagerInterface storageManager,
			IIndexRegistryProvider<IIndex> indexRegistryProvider,
			IFileSplitProvider fileSplitProvider,
			ITreeIndexFrameFactory interiorFrameFactory,
			ITreeIndexFrameFactory leafFrameFactory, ITypeTrait[] typeTraits,
			IBinaryComparatorFactory[] comparatorFactories,
			IIndexDataflowHelperFactory dataflowHelperFactory) {
		super(spec, inputArity, outputArity);
		this.fileSplitProvider = fileSplitProvider;
		this.storageManager = storageManager;
		this.indexRegistryProvider = indexRegistryProvider;
		this.interiorFrameFactory = interiorFrameFactory;
		this.leafFrameFactory = leafFrameFactory;
		this.typeTraits = typeTraits;
		this.comparatorFactories = comparatorFactories;
		this.dataflowHelperFactory = dataflowHelperFactory;
		if (outputArity > 0) {
			recordDescriptors[0] = recDesc;
		}
	}

	@Override
	public IFileSplitProvider getFileSplitProvider() {
		return fileSplitProvider;
	}

	@Override
	public IBinaryComparatorFactory[] getTreeIndexComparatorFactories() {
		return comparatorFactories;
	}

	@Override
	public ITypeTrait[] getTreeIndexTypeTraits() {
		return typeTraits;
	}

	@Override
	public ITreeIndexFrameFactory getTreeIndexInteriorFactory() {
		return interiorFrameFactory;
	}

	@Override
	public ITreeIndexFrameFactory getTreeIndexLeafFactory() {
		return leafFrameFactory;
	}

	@Override
	public IStorageManagerInterface getStorageManager() {
		return storageManager;
	}

	@Override
	public IIndexRegistryProvider<IIndex> getIndexRegistryProvider() {
		return indexRegistryProvider;
	}

	@Override
	public RecordDescriptor getRecordDescriptor() {
		return recordDescriptors[0];
	}

	@Override
	public IIndexDataflowHelperFactory getIndexDataflowHelperFactory() {
		return dataflowHelperFactory;
	}
}