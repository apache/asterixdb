/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.storage.am.lsmtree.common;

import static org.junit.Assert.fail;

import org.junit.Test;

import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.api.PageAllocationException;
import edu.uci.ics.hyracks.storage.am.common.frames.LIFOMetaDataFrameFactory;
import edu.uci.ics.hyracks.storage.am.lsmtree.common.freepage.InMemoryFreePageManager;

public class InMemoryFreePageManagerTest{

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
