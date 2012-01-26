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

package edu.uci.ics.hyracks.storage.am.btree.tests;

import java.util.TreeSet;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;

@SuppressWarnings("rawtypes")
public interface IOrderedIndexTestContext {
    public int getFieldCount();
    
    public int getKeyFieldCount();
        
	public ISerializerDeserializer[] getFieldSerdes();
    
    public ITreeIndexAccessor getIndexAccessor();
    
    public ITreeIndex getIndex();

    public ArrayTupleReference getTuple();
    
    public ArrayTupleBuilder getTupleBuilder();

    public IBinaryComparator[] getComparators();
    
    public void insertIntCheckTuple(int[] fieldValues);
    
    public void insertStringCheckTuple(String[] fieldValues);
    
    public TreeSet<CheckTuple> getCheckTuples();
}
