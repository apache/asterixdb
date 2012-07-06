package edu.uci.ics.hyracks.storage.am.lsm.common;

import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.TreeFactory;

public class DummyTreeFactory extends TreeFactory<ITreeIndex> {

    public DummyTreeFactory() {
        super(null, null, null, null, null, null, 0);
    }

    @Override
    public ITreeIndex createIndexInstance(FileReference file) {
        return null;
    }

}
