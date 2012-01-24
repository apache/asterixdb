package edu.uci.ics.hyracks.storage.am.lsmtree.freepage;

import static org.junit.Assert.fail;

import org.junit.Test;

import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.PageAllocationException;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;

public class InMemoryFreePageManagerTest{

	//Get the free pages
    @Test
    public void InMemoryFreePageManagerTest01() throws Exception {
    	
        ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();
        IFreePageManager memFreePageManager = new InMemoryFreePageManager(10, metaFrameFactory);
    	
        try{
	        for(int i = 0; i < 5; i++) {
	        	memFreePageManager.getFreePage(null);
	        }
        }
        catch (PageAllocationException e){
        	System.out.println("Catch exception: " + e);
        	return;
        }
        catch (Exception e) {
        	fail("Unexpected exception!");
        }
    } 
	
	//Get free pages more than the max capacity
    //expect PageAllocationException
    @Test
    public void InMemoryFreePageManagerTest02() throws Exception {
    	
        ITreeIndexMetaDataFrameFactory metaFrameFactory = new LIFOMetaDataFrameFactory();
        IFreePageManager memFreePageManager = new InMemoryFreePageManager(10, metaFrameFactory);
    	
        try{
	        for(int i = 0; i < 20; i++) {
	        	memFreePageManager.getFreePage(null);
	        }
        }
        catch (PageAllocationException e){
        	System.out.println("Catch exception: " + e);
        	return;
        }
        catch (Exception e) {
        	fail("Unexpected exception!");
        }
    } 
}
