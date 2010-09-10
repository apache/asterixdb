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
import edu.uci.ics.hyracks.storage.am.btree.frames.FieldPrefixNSMLeafFrame;

// TODO: make members private, only for debugging now
public class FieldPrefixFieldIterator implements IFieldIterator {
    public int recSlotOff = -1;
    public int recOff = -1;
    public int prefixSlotNum = FieldPrefixSlotManager.RECORD_UNCOMPRESSED;
    public int numPrefixFields = 0;
    public IFieldAccessor[] fields;
    public FieldPrefixNSMLeafFrame frame;
    
    public int currentField = -1;
    public int recOffRunner = -1;
    
    public FieldPrefixFieldIterator() {    	
    }
    
    public FieldPrefixFieldIterator(IFieldAccessor[] fields, FieldPrefixNSMLeafFrame frame) {
        this.fields = fields;
        this.frame = frame;
    }
    
    public void setFrame(IBTreeFrame frame) {
        this.frame = (FieldPrefixNSMLeafFrame)frame;        
    }
    
    public void setFields(IFieldAccessor[] fields) {
        this.fields = fields;
    }
    
    public void openRecSlotNum(int recSlotNum) {          
        openRecSlotOff(frame.slotManager.getRecSlotOff(recSlotNum));         
    }
    
    public void openRecSlotOff(int recSlotOff) {
        this.recSlotOff = recSlotOff;
        reset();
    }
    
    // re-position to first field
    public void reset() {
        currentField = 0;
        numPrefixFields = 0;
        int recSlot = frame.getBuffer().getInt(recSlotOff);
        prefixSlotNum = frame.slotManager.decodeFirstSlotField(recSlot);
        recOff = frame.slotManager.decodeSecondSlotField(recSlot);
                
        // position to prefix records first (if record is compressed)
        if(prefixSlotNum != FieldPrefixSlotManager.RECORD_UNCOMPRESSED) {
            int prefixSlotOff = frame.slotManager.getPrefixSlotOff(prefixSlotNum);
            int prefixSlot = frame.getBuffer().getInt(prefixSlotOff);
            numPrefixFields = frame.slotManager.decodeFirstSlotField(prefixSlot);
            recOffRunner = frame.slotManager.decodeSecondSlotField(prefixSlot);               
        }
        else {
            recOffRunner = recOff;
        }        
    }
    
    public void nextField() {                   
            	
    	// if we have passed the prefix fields of any of the two records, position them to the suffix record
        if(currentField+1 == numPrefixFields) recOffRunner = recOff;
        else recOffRunner += fields[currentField].getLength(frame.getBuffer().array(), recOffRunner);
        currentField++;        
    }
    
    public int getFieldOff() {
        return recOffRunner;
    }        
    
    public int getFieldSize() {
        return fields[currentField].getLength(frame.getBuffer().array(), recOffRunner);                
    }
    
    // TODO: this is the simplest implementation, could be improved to minimize the number calls to System.arrayCopy()
    // copies the fields [startField, endField] into dest at position destOff
    // returns the total number of bytes written
    // this operation does not change the instances state
    public int copyFields(int startField, int endField, byte[] dest, int destOff) {
        
        // remember state
        int oldCurrentField = currentField;
        int oldRecOffRunner = recOffRunner;
        
        // do we need to reposition from start?
        if(currentField != startField) {
            reset();
            while(currentField != startField) nextField();
        }
        
        // perform copy
        int bytesWritten = 0;
        for(int i = startField; i <= endField; i++) {
            int fieldSize = getFieldSize();                        
            System.arraycopy(frame.getBuffer().array(), recOffRunner, dest, destOff, fieldSize);
            bytesWritten += fieldSize;             
            destOff += fieldSize;
            nextField();
        }
                    
        // restore original state
        currentField = oldCurrentField;
        recOffRunner = oldRecOffRunner;
                    
        return bytesWritten;
    }
    
    @Override
    public ByteBuffer getBuffer() {
    	return frame.getBuffer();
    }
}
