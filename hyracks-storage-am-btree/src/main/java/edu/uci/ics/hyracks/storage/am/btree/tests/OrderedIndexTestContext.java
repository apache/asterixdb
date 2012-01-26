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

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;

@SuppressWarnings("rawtypes")
public abstract class OrderedIndexTestContext implements IOrderedIndexTestContext {
    protected final ISerializerDeserializer[] fieldSerdes;
    protected final ITreeIndex treeIndex;
    protected final ArrayTupleBuilder tupleBuilder;
    protected final ArrayTupleReference tuple = new ArrayTupleReference();
    protected final TreeSet<CheckTuple> checkTuples = new TreeSet<CheckTuple>();
    protected final ITreeIndexAccessor indexAccessor;

    public OrderedIndexTestContext(ISerializerDeserializer[] fieldSerdes, ITreeIndex treeIndex) {
        this.fieldSerdes = fieldSerdes;
        this.treeIndex = treeIndex;
        this.indexAccessor = treeIndex.createAccessor();
        this.tupleBuilder = new ArrayTupleBuilder(fieldSerdes.length);
    }

    @Override
    public int getFieldCount() {
        return fieldSerdes.length;
    }

    @Override
    public ITreeIndexAccessor getIndexAccessor() {
        return indexAccessor;
    }

    @Override
    public ArrayTupleReference getTuple() {
        return tuple;
    }

    @Override
    public ArrayTupleBuilder getTupleBuilder() {
        return tupleBuilder;
    }

    @Override
    public void insertIntCheckTuple(int[] fieldValues) {        
        CheckTuple<Integer> checkTuple = new CheckTuple<Integer>(getFieldCount(), getKeyFieldCount());
        for(int v : fieldValues) {
            checkTuple.add(v);
        }
        checkTuples.add(checkTuple);
    }

    @Override
    public void insertStringCheckTuple(String[] fieldValues) {
        CheckTuple<String> checkTuple = new CheckTuple<String>(getFieldCount(), getKeyFieldCount());
        for(String s : fieldValues) {
            checkTuple.add((String)s);
        }
        checkTuples.add(checkTuple);
    }

    @Override
    public ISerializerDeserializer[] getFieldSerdes() {
        return fieldSerdes;
    }

    @Override
    public TreeSet<CheckTuple> getCheckTuples() {
        return checkTuples;
    }

    @Override
    public ITreeIndex getIndex() {
        return treeIndex;
    }
}