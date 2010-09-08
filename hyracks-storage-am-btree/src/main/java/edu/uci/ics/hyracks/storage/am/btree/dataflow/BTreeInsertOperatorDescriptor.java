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

public class BTreeInsertOperatorDescriptor extends AbstractBTreeOperatorDescriptor {
	
	private static final long serialVersionUID = 1L;
	
	private final int[] keyFields;
	private final int[] payloadFields;
	
	public BTreeInsertOperatorDescriptor(JobSpecification spec,
			IFileSplitProvider fileSplitProvider, RecordDescriptor recDesc,
			IBufferCacheProvider bufferCacheProvider,
			IBTreeRegistryProvider btreeRegistryProvider, int btreeFileId,
			String btreeFileName, IBTreeInteriorFrameFactory interiorFactory,
			IBTreeLeafFrameFactory leafFactory, IFieldAccessorFactory[] fieldAccessorFactories, 
			IBinaryComparatorFactory[] comparatorFactories,			
			int[] keyFields, int[] payloadFields) {
		super(spec, 1, 1, fileSplitProvider, recDesc, bufferCacheProvider,
				btreeRegistryProvider, btreeFileId, btreeFileName, interiorFactory,
				leafFactory, fieldAccessorFactories, comparatorFactories);
		this.keyFields = keyFields;
		this.payloadFields = payloadFields;
	}
	
	@Override
	public IOperatorNodePushable createPushRuntime(IHyracksContext ctx,
			IOperatorEnvironment env,
			IRecordDescriptorProvider recordDescProvider, int partition,
			int nPartitions) {
		return new BTreeInsertOperatorNodePushable(this, ctx, keyFields, payloadFields, recordDescProvider);
	}
	
}
