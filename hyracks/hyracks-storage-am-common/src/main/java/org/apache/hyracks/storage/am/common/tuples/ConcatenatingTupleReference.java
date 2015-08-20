/*
 * Copyright 2009-2013 by The Regents of the University of California
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

package edu.uci.ics.hyracks.storage.am.common.tuples;

import java.util.Arrays;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * WARNING: getFieldData(), getFieldStart() and getFieldLength() are log and not constant time.
 */
public class ConcatenatingTupleReference implements ITupleReference {

    private final ITupleReference[] tuples;
    private final int[] fieldCounts;
    private int numTuples;
    private int totalFieldCount;
    
    public ConcatenatingTupleReference(int maxTuples) {
        tuples = new ITupleReference[maxTuples];
        fieldCounts = new int[maxTuples];
        reset();        
    }
    
    public void reset() {
        numTuples = 0;
        totalFieldCount = 0;
    }
    
    public void addTuple(ITupleReference tuple) {
        tuples[numTuples] = tuple;
        totalFieldCount += tuple.getFieldCount();
        if (numTuples > 0) {
            fieldCounts[numTuples] = fieldCounts[numTuples - 1] + tuple.getFieldCount();
        } else {
            fieldCounts[numTuples] = tuple.getFieldCount();
        }
        ++numTuples;
    }
    
    public void removeLastTuple() {
        if (numTuples > 0) {
            ITupleReference lastTuple = tuples[--numTuples];
            totalFieldCount -= lastTuple.getFieldCount();
        }
    }
    
    public int getNumTuples() {
        return numTuples;
    }
    
    public boolean hasMaxTuples() {
        return numTuples == tuples.length;
    }
    
    @Override
    public int getFieldCount() {
        return totalFieldCount;
    }

    @Override
    public byte[] getFieldData(int fIdx) {
        int tupleIndex = getTupleIndex(fIdx);
        int fieldIndex = getFieldIndex(tupleIndex, fIdx);
        return tuples[tupleIndex].getFieldData(fieldIndex);
    }

    @Override
    public int getFieldStart(int fIdx) {
        int tupleIndex = getTupleIndex(fIdx);
        int fieldIndex = getFieldIndex(tupleIndex, fIdx);
        return tuples[tupleIndex].getFieldStart(fieldIndex);
    }

    @Override
    public int getFieldLength(int fIdx) {
        int tupleIndex = getTupleIndex(fIdx);
        int fieldIndex = getFieldIndex(tupleIndex, fIdx);
        return tuples[tupleIndex].getFieldLength(fieldIndex);
    }
    
    private int getTupleIndex(int fIdx) {
        int tupleIndex = Arrays.binarySearch(fieldCounts, 0, numTuples - 1, fIdx);
        if (tupleIndex < 0) {
            tupleIndex = -tupleIndex - 1;
        } else {
            ++tupleIndex;
        }
        return tupleIndex;
    }
    
    private int getFieldIndex(int tupleIndex, int fIdx) {
        int fieldIndex = -1;
        if (tupleIndex > 0) {
            fieldIndex = fIdx - fieldCounts[tupleIndex - 1]; 
        } else {
            fieldIndex = fIdx;
        }
        return fieldIndex;
    }
}
