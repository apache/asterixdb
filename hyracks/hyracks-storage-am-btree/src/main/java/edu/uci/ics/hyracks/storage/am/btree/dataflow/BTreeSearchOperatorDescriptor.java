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

public class BTreeSearchOperatorDescriptor extends AbstractBTreeOperatorDescriptor {

	private static final long serialVersionUID = 1L;

	private boolean isForward;
	private byte[] lowKey;
	private byte[] highKey;
	private int searchKeyFields;
	
	public BTreeSearchOperatorDescriptor(JobSpecification spec, IFileSplitProvider fileSplitProvider, RecordDescriptor recDesc, IBufferCacheProvider bufferCacheProvider, IBTreeRegistryProvider btreeRegistryProvider,  int btreeFileId, String btreeFileName, IBTreeInteriorFrameFactory interiorFactory, IBTreeLeafFrameFactory leafFactory, IFieldAccessorFactory[] fieldAccessorFactories, IBinaryComparatorFactory[] comparatorFactories, boolean isForward, byte[] lowKey, byte[] highKey, int searchKeyFields) {
		super(spec, 0, 1, fileSplitProvider, recDesc, bufferCacheProvider, btreeRegistryProvider, btreeFileId, btreeFileName, interiorFactory, leafFactory, fieldAccessorFactories, comparatorFactories);
		this.isForward = isForward;
		this.lowKey = lowKey;
		this.highKey = highKey;
		this.searchKeyFields = searchKeyFields;
	}
	
	@Override
	public IOperatorNodePushable createPushRuntime(final IHyracksContext ctx, final IOperatorEnvironment env,
			IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
		return new BTreeSearchOperatorNodePushable(this, ctx, isForward, lowKey, highKey, searchKeyFields);
	}	
}
