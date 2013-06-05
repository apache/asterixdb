package edu.uci.ics.asterix.common.dataflow;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManagerProvider;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallbackFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITupleFilterFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.dataflow.LSMTreeIndexInsertUpdateDeleteOperatorDescriptor;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;

public class AsterixLSMTreeInsertDeleteOperatorDescriptor extends LSMTreeIndexInsertUpdateDeleteOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private final boolean isPrimary;

    public AsterixLSMTreeInsertDeleteOperatorDescriptor(IOperatorDescriptorRegistry spec, RecordDescriptor recDesc,
            IStorageManagerInterface storageManager, IIndexLifecycleManagerProvider lifecycleManagerProvider,
            IFileSplitProvider fileSplitProvider, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] comparatorFactories, int[] bloomFilterKeyFields, int[] fieldPermutation,
            IndexOperation op, IIndexDataflowHelperFactory dataflowHelperFactory,
            ITupleFilterFactory tupleFilterFactory,
            IModificationOperationCallbackFactory modificationOpCallbackProvider, boolean isPrimary) {
        super(spec, recDesc, storageManager, lifecycleManagerProvider, fileSplitProvider, typeTraits,
                comparatorFactories, bloomFilterKeyFields, fieldPermutation, op, dataflowHelperFactory,
                tupleFilterFactory, modificationOpCallbackProvider);
        this.isPrimary = isPrimary;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        return new AsterixLSMInsertDeleteOperatorNodePushable(this, ctx, partition, fieldPermutation,
                recordDescProvider, op, isPrimary);
    }

}
