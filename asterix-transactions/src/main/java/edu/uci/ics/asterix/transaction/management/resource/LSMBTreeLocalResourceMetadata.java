package edu.uci.ics.asterix.transaction.management.resource;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;

public class LSMBTreeLocalResourceMetadata implements ILocalResourceMetadata {

    private final ITypeTraits[] typeTraits;
    private final IBinaryComparatorFactory[] cmpFactories;
    private final boolean isPrimary;

    public LSMBTreeLocalResourceMetadata(ITypeTraits[] typeTraits, IBinaryComparatorFactory[] cmpFactories,
            boolean isPrimary) {
        this.typeTraits = typeTraits;
        this.cmpFactories = cmpFactories;
        this.isPrimary = isPrimary;
    }

    @Override
    public ILSMIndex createIndexInstance() {
        return null;
    }

}
