/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hyracks.storage.am.common.tuples;

import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

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

    /**
     * Right now this class is only used by inverted index, and only contains 2 tuples.
     * As a result, sequential search would be more efficient than binary search
     */
    private int getTupleIndex(int fIdx) {
        for (int i = 0; i < numTuples; i++) {
            if (fIdx < fieldCounts[i]) {
                return i;
            }
        }
        throw new IllegalArgumentException("Illegal field index " + fIdx);
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
