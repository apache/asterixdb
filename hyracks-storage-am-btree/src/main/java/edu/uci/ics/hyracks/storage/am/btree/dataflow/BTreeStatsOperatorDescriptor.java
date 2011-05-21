package edu.uci.ics.hyracks.storage.am.btree.dataflow;

import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrameFactory;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;

public class BTreeStatsOperatorDescriptor extends AbstractBTreeOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    
    public BTreeStatsOperatorDescriptor(JobSpecification spec, IStorageManagerInterface storageManager,
            IBTreeRegistryProvider btreeRegistryProvider, IFileSplitProvider fileSplitProvider,
            IBTreeInteriorFrameFactory interiorFactory, IBTreeLeafFrameFactory leafFactory, ITypeTrait[] typeTraits,
            IBinaryComparatorFactory[] comparatorFactories) {
        super(spec, 0, 0, null, storageManager, btreeRegistryProvider, fileSplitProvider, interiorFactory, leafFactory,
                typeTraits, comparatorFactories);
    }
    
    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksStageletContext ctx, IOperatorEnvironment env,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        return new BTreeStatsOperatorNodePushable(this, ctx, partition);
    }
}
