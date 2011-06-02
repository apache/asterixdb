package edu.uci.ics.hyracks.storage.am.invertedindex.dataflow;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.common.dataflow.ITreeIndexOperatorDescriptorHelper;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexRegistryProvider;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.InvertedIndex;

public interface IInvertedIndexOperatorDescriptorHelper extends ITreeIndexOperatorDescriptorHelper {            
    public IFileSplitProvider getInvIndexFileSplitProvider();

    public IBinaryComparatorFactory[] getInvIndexComparatorFactories();

    public ITypeTrait[] getInvIndexTypeTraits();
    
    public IIndexRegistryProvider<InvertedIndex> getInvIndexRegistryProvider();
}
