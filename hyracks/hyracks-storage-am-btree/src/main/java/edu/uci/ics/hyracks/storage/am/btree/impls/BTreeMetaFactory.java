package edu.uci.ics.asterix.indexing.btree.impls;

import edu.uci.ics.asterix.indexing.btree.interfaces.IBTreeFrameMeta;
import edu.uci.ics.asterix.indexing.btree.interfaces.IBTreeFrameMetaFactory;

public class BTreeMetaFactory implements IBTreeFrameMetaFactory {
    @Override
    public IBTreeFrameMeta getFrame() {     
        return new BTreeMeta();
    }   
}
