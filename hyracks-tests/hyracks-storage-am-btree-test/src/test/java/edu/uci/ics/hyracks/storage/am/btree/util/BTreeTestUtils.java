package edu.uci.ics.hyracks.storage.am.btree.util;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.dataflow.common.util.SerdeUtils;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeInteriorFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;

@SuppressWarnings("rawtypes")
public class BTreeTestUtils {
    public static BTreeTestContext createBTreeTestContext(IBufferCache bufferCache, int btreeFileId, ISerializerDeserializer[] fieldSerdes, int numKeyFields, BTreeLeafFrameType leafType) throws Exception {        
        ITypeTraits[] typeTraits = SerdeUtils.serdesToTypeTraits(fieldSerdes);
        IBinaryComparator[] cmps = SerdeUtils.serdesToComparators(fieldSerdes, numKeyFields);
        
        BTree btree = BTreeUtils.createBTree(bufferCache, btreeFileId, typeTraits, cmps, leafType);
        btree.create(btreeFileId);
        btree.open(btreeFileId);
        ITreeIndexAccessor indexAccessor = btree.createAccessor();
        
        IBTreeLeafFrame leafFrame = (IBTreeLeafFrame) btree.getLeafFrameFactory().createFrame();
        IBTreeInteriorFrame interiorFrame = (IBTreeInteriorFrame) btree.getInteriorFrameFactory().createFrame();
        ITreeIndexMetaDataFrame metaFrame = btree.getFreePageManager().getMetaDataFrameFactory().createFrame();
        BTreeTestContext testCtx = new BTreeTestContext(bufferCache, fieldSerdes, btree, leafFrame, interiorFrame, metaFrame, indexAccessor);
        return testCtx;
    }
}
