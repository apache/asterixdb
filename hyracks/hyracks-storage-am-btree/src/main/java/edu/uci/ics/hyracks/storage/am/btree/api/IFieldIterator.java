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

package edu.uci.ics.hyracks.storage.am.btree.api;

import java.nio.ByteBuffer;

public interface IFieldIterator {
	public void setFrame(IBTreeFrame frame);

    public void setFields(IFieldAccessor[] fields);
    
    public void openRecSlotNum(int recSlotNum);
    
    public void openRecSlotOff(int recSlotOff);
    
    public void reset();
    
    public void nextField();
    
    public int getFieldOff();
    
    public int getFieldSize();
    
    public int copyFields(int startField, int endField, byte[] dest, int destOff);   
    
    public ByteBuffer getBuffer();
}
