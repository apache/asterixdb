package edu.uci.ics.hyracks.storage.am.btree.dataflow;

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
import edu.uci.ics.hyracks.storage.am.common.api.ITupleUpdaterFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexRegistryProvider;
import edu.uci.ics.hyracks.storage.am.common.dataflow.ITreeIndexOpHelperFactory;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;

public class BTreeUpdateSearchOperatorDescriptor extends BTreeSearchOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    
    private final ITupleUpdaterFactory tupleUpdaterFactory;
    
    public BTreeUpdateSearchOperatorDescriptor(JobSpecification spec, RecordDescriptor recDesc,
            IStorageManagerInterface storageManager, IIndexRegistryProvider<ITreeIndex> treeIndexRegistryProvider,
            IFileSplitProvider fileSplitProvider, ITreeIndexFrameFactory interiorFrameFactory,
            ITreeIndexFrameFactory leafFrameFactory, ITypeTrait[] typeTraits,
            IBinaryComparatorFactory[] comparatorFactories, boolean isForward, int[] lowKeyFields, int[] highKeyFields,
            boolean lowKeyInclusive, boolean highKeyInclusive, ITreeIndexOpHelperFactory opHelperFactory,
            ITupleUpdaterFactory tupleUpdaterFactory) {
        super(spec, recDesc, storageManager, treeIndexRegistryProvider, fileSplitProvider, interiorFrameFactory,
                leafFrameFactory, typeTraits, comparatorFactories, isForward, lowKeyFields, highKeyFields, lowKeyInclusive,
                highKeyInclusive, opHelperFactory);
        this.tupleUpdaterFactory = tupleUpdaterFactory;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx, IRecordDescriptorProvider recordDescProvider,
            int partition, int nPartitions) {
        return new BTreeUpdateSearchOperatorNodePushable(this, ctx, partition, recordDescProvider, isForward, lowKeyFields,
                highKeyFields, lowKeyInclusive, highKeyInclusive, tupleUpdaterFactory.createTupleUpdater());
    }
}
