package edu.uci.ics.hyracks.storage.am.common.dataflow;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;

public class TreeIndexStatsOperatorDescriptor extends
		AbstractTreeIndexOperatorDescriptor {

	private static final long serialVersionUID = 1L;

	public TreeIndexStatsOperatorDescriptor(JobSpecification spec,
			IStorageManagerInterface storageManager,
			IIndexRegistryProvider<ITreeIndex> treeIndexRegistryProvider,
			IFileSplitProvider fileSplitProvider,
			ITreeIndexFrameFactory interiorFrameFactory,
			ITreeIndexFrameFactory leafFrameFactory, ITypeTrait[] typeTraits,
			IBinaryComparatorFactory[] comparatorFactories,
			ITreeIndexOpHelperFactory opHelperFactory) {
		super(spec, 0, 0, null, storageManager, treeIndexRegistryProvider,
				fileSplitProvider, interiorFrameFactory, leafFrameFactory,
				typeTraits, comparatorFactories, opHelperFactory);
	}

	@Override
	public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
			IRecordDescriptorProvider recordDescProvider,
			int partition, int nPartitions) {
		return new TreeIndexStatsOperatorNodePushable(this, ctx, partition);
	}
}