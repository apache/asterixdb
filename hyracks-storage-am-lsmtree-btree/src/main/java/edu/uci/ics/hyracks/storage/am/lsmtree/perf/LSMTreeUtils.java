package edu.uci.ics.hyracks.storage.am.lsmtree.perf;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.storage.am.btree.exceptions.BTreeException;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeLeafFrameType;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeNSMInteriorFrameFactory;
import edu.uci.ics.hyracks.storage.am.btree.util.BTreeUtils;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.freepage.LinkedListFreePageManagerFactory;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsmtree.common.freepage.InMemoryFreePageManager;
import edu.uci.ics.hyracks.storage.am.lsmtree.impls.BTreeFactory;
import edu.uci.ics.hyracks.storage.am.lsmtree.impls.LSMTree;
import edu.uci.ics.hyracks.storage.am.lsmtree.tuples.LSMEntireTupleWriterFactory;
import edu.uci.ics.hyracks.storage.am.lsmtree.tuples.LSMTypeAwareTupleWriterFactory;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapManager;

public class LSMTreeUtils {
    public static LSMTree createLSMTree(IBufferCache memCache, IBufferCache bufferCache, int lsmtreeFileId, ITypeTraits[] typeTraits, IBinaryComparator[] cmps, BTreeLeafFrameType leafType, IFileMapManager fileMapManager) throws BTreeException {
    	MultiComparator cmp = new MultiComparator(cmps);

		LSMTypeAwareTupleWriterFactory insertTupleWriterFactory = new LSMTypeAwareTupleWriterFactory(typeTraits, false);
		LSMTypeAwareTupleWriterFactory deleteTupleWriterFactory = new LSMTypeAwareTupleWriterFactory(typeTraits, true);

		ITreeIndexFrameFactory insertLeafFrameFactory = BTreeUtils.getLeafFrameFactory(insertTupleWriterFactory,leafType);
		ITreeIndexFrameFactory deleteLeafFrameFactory = BTreeUtils.getLeafFrameFactory(deleteTupleWriterFactory,leafType);
		ITreeIndexFrameFactory interiorFrameFactory = new BTreeNSMInteriorFrameFactory(insertTupleWriterFactory);
		ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();
		InMemoryFreePageManager memFreePageManager = new InMemoryFreePageManager(memCache.getNumPages(), metaFrameFactory);
	
		// For the Flush Mechanism
		LSMEntireTupleWriterFactory flushTupleWriterFactory = new LSMEntireTupleWriterFactory(typeTraits);
		ITreeIndexFrameFactory flushLeafFrameFactory = BTreeUtils.getLeafFrameFactory(flushTupleWriterFactory,leafType); 
		LinkedListFreePageManagerFactory freePageManagerFactory = new LinkedListFreePageManagerFactory(bufferCache, metaFrameFactory);
		BTreeFactory bTreeFactory = new BTreeFactory(bufferCache, freePageManagerFactory, cmp, typeTraits.length, interiorFrameFactory, flushLeafFrameFactory);
        
        LSMTree lsmtree = new LSMTree(memCache, bufferCache, typeTraits.length, cmp, memFreePageManager, 
    			 interiorFrameFactory, insertLeafFrameFactory, deleteLeafFrameFactory, bTreeFactory, fileMapManager);
        return lsmtree;
    }
}
