package edu.uci.ics.hyracks.storage.am.lsm.impls;

import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeOpContext;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;

public final class LSMTreeOpContext extends BTreeOpContext {

	public ITreeIndexFrameFactory insertLeafFrameFactory;
	public ITreeIndexFrameFactory deleteLeafFrameFactory;
	public IBTreeLeafFrame insertLeafFrame;
	public IBTreeLeafFrame deleteLeafFrame;
	public final BTree.BTreeAccessor memBtreeAccessor;

	public LSMTreeOpContext(BTree.BTreeAccessor memBtreeAccessor, ITreeIndexFrameFactory insertLeafFrameFactory,
			ITreeIndexFrameFactory deleteLeafFrameFactory, ITreeIndexFrameFactory interiorFrameFactory,
			ITreeIndexMetaDataFrame metaFrame, MultiComparator cmp) {
		super(insertLeafFrameFactory, interiorFrameFactory, metaFrame, cmp);		
		
		this.memBtreeAccessor = memBtreeAccessor;
        // Overwrite the BTree accessor's op context with our LSMTreeOpContext.
		this.memBtreeAccessor.setOpContext(this);
		
		this.insertLeafFrameFactory = insertLeafFrameFactory;
		this.deleteLeafFrameFactory = deleteLeafFrameFactory;
		this.insertLeafFrame = (IBTreeLeafFrame) insertLeafFrameFactory.createFrame();
		this.deleteLeafFrame = (IBTreeLeafFrame) deleteLeafFrameFactory.createFrame();
		
		if (insertLeafFrame != null) {
			insertLeafFrame.setMultiComparator(cmp);
        }
		
        if (deleteLeafFrame != null) {
        	deleteLeafFrame.setMultiComparator(cmp);
        }
        
        reset(op);
	}

    @Override
    public void reset(IndexOp newOp) {
    	super.reset(newOp);
    	if(newOp == IndexOp.INSERT) {
    		setInsertMode();
    	}
    	if(newOp == IndexOp.DELETE) {
    		super.reset(IndexOp.INSERT);
    		setDeleteMode();
    	}
    }
	
	public void setInsertMode() {
		this.leafFrame = insertLeafFrame;
		leafFrameFactory = insertLeafFrameFactory;
	}
	
	public void setDeleteMode() {
		this.leafFrame = deleteLeafFrame;
		leafFrameFactory = deleteLeafFrameFactory;
	}
	
}