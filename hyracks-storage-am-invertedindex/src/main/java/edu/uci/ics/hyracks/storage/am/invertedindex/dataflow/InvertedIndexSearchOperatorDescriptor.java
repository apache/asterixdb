package edu.uci.ics.hyracks.storage.am.invertedindex.dataflow;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexRegistryProvider;
import edu.uci.ics.hyracks.storage.am.common.dataflow.ITreeIndexOpHelperFactory;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndexSearchModifier;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndexSearchModifierFactory;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.InvertedIndex;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IBinaryTokenizer;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IBinaryTokenizerFactory;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;

public class InvertedIndexSearchOperatorDescriptor extends AbstractInvertedIndexOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private final int queryField;
    private final IBinaryTokenizerFactory queryTokenizerFactory;
    private final IInvertedIndexSearchModifierFactory searchModifierFactory;

    public InvertedIndexSearchOperatorDescriptor(JobSpecification spec, int queryField,
            IStorageManagerInterface storageManager, IFileSplitProvider btreeFileSplitProvider,
            IIndexRegistryProvider<ITreeIndex> treeIndexRegistryProvider,
            ITreeIndexFrameFactory btreeInteriorFrameFactory, ITreeIndexFrameFactory btreeLeafFrameFactory,
            ITypeTrait[] btreeTypeTraits, IBinaryComparatorFactory[] btreeComparatorFactories,
            ITreeIndexOpHelperFactory btreeOpHelperFactory, IFileSplitProvider invListsFileSplitProvider,
            IIndexRegistryProvider<InvertedIndex> invIndexRegistryProvider, ITypeTrait[] invListsTypeTraits,
            IBinaryComparatorFactory[] invListsComparatorFactories,
            IInvertedIndexSearchModifierFactory searchModifierFactory, IBinaryTokenizerFactory queryTokenizerFactory,
            RecordDescriptor recDesc) {
        super(spec, 1, 1, recDesc, storageManager, btreeFileSplitProvider, treeIndexRegistryProvider,
                btreeInteriorFrameFactory, btreeLeafFrameFactory, btreeTypeTraits, btreeComparatorFactories,
                btreeOpHelperFactory, invListsFileSplitProvider, invIndexRegistryProvider, invListsTypeTraits,
                invListsComparatorFactories);
        this.queryField = queryField;
        this.searchModifierFactory = searchModifierFactory;
        this.queryTokenizerFactory = queryTokenizerFactory;
    }

	@Override
	public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
			IRecordDescriptorProvider recordDescProvider, int partition,
			int nPartitions) throws HyracksDataException {
		IBinaryTokenizer tokenizer = queryTokenizerFactory.createTokenizer();
        IInvertedIndexSearchModifier searchModifier = searchModifierFactory.createSearchModifier();
        return new InvertedIndexSearchOperatorNodePushable(this, ctx, partition, queryField, searchModifier, tokenizer,
                recordDescProvider);
	}
}
