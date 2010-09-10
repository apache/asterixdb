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

package edu.uci.ics.hyracks.storage.am.btree.impls;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeFrame;
import edu.uci.ics.hyracks.storage.am.btree.api.IFieldAccessor;
import edu.uci.ics.hyracks.storage.am.btree.api.IFieldIterator;
import edu.uci.ics.hyracks.storage.am.btree.frames.NSMFrame;

public class NSMFieldIterator implements IFieldIterator {
    public int recOff = -1;
    public IFieldAccessor[] fields;
    public NSMFrame frame;
    
    public int currentField = -1;
    public int recOffRunner = -1;
		
	public NSMFieldIterator() {    	
    }
    
    public NSMFieldIterator(IFieldAccessor[] fields, NSMFrame frame) {
        this.fields = fields;
        this.frame = frame;
    }
    
    public void setFrame(NSMFrame frame) {
        this.frame = frame;        
    }
    
    public void setFields(IFieldAccessor[] fields) {
        this.fields = fields;
    }
    
    public void openRecSlotNum(int recSlotNum) {                 
    	int recSlotOff = frame.getSlotManager().getSlotOff(recSlotNum);
    	openRecSlotOff(recSlotOff);
    }
    
    public void openRecSlotOff(int recSlotOff) {
    	recOff = frame.getSlotManager().getRecOff(recSlotOff);
        reset();
    }
    
    // re-position to first field
    public void reset() {
        currentField = 0;
        recOffRunner = recOff;               
    }
    
    public void nextField() {                   
    	recOffRunner += fields[currentField].getLength(frame.getBuffer().array(), recOffRunner);    
    }
    
    public int getFieldOff() {
        return recOffRunner;
    }        
    
    public int getFieldSize() {
        return fields[currentField].getLength(frame.getBuffer().array(), recOffRunner);                
    }
    
    public int copyFields(int startField, int endField, byte[] dest, int destOff) {
    
    	return 0;
    }

	@Override
	public void setFrame(IBTreeFrame frame) {
		this.frame = (NSMFrame)frame;
	}	
	
	@Override
    public ByteBuffer getBuffer() {
    	return frame.getBuffer();
    }
}
