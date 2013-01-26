package edu.uci.ics.asterix.transaction.management.resource;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ILinearizeComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreePolicyType;

public class LSMRTreeLocalResourceMetadata implements ILocalResourceMetadata {

    private final ITypeTraits[] typeTraits;
    private final IBinaryComparatorFactory[] rtreeCmpFactories;
    private final IBinaryComparatorFactory[] btreeCmpFactories;
    private final IPrimitiveValueProviderFactory[] valueProviderFactories;
    private final RTreePolicyType rtreePolicyType;
    private final ILinearizeComparatorFactory linearizeCmpFactory;

    public LSMRTreeLocalResourceMetadata(ITypeTraits[] typeTraits, IBinaryComparatorFactory[] rtreeCmpFactories,
            IBinaryComparatorFactory[] btreeCmpFactories, IPrimitiveValueProviderFactory[] valueProviderFactories,
            RTreePolicyType rtreePolicyType, ILinearizeComparatorFactory linearizeCmpFactory) {
        this.typeTraits = typeTraits;
        this.rtreeCmpFactories = rtreeCmpFactories;
        this.btreeCmpFactories = btreeCmpFactories;
        this.valueProviderFactories = valueProviderFactories;
        this.rtreePolicyType = rtreePolicyType;
        this.linearizeCmpFactory = linearizeCmpFactory;
    }

    @Override
    public ILSMIndex createIndexInstance() {
        // TODO Auto-generated method stub
        return null;
    }
}
