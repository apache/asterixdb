package edu.uci.ics.hyracks.storage.am.btree.impls;

import edu.uci.ics.hyracks.storage.am.btree.interfaces.IBTreeFrameMeta;
import edu.uci.ics.hyracks.storage.am.btree.interfaces.IBTreeFrameMetaFactory;

public class BTreeMetaFactory implements IBTreeFrameMetaFactory {
    @Override
    public IBTreeFrameMeta getFrame() {     
        return new BTreeMeta();
    }   
}
