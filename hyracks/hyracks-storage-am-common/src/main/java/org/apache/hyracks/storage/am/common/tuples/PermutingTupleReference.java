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

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

public class PermutingTupleReference implements ITupleReference {

    private final int[] fieldPermutation;
    private ITupleReference sourceTuple;
    
    public PermutingTupleReference(int[] fieldPermutation) {
        this.fieldPermutation = fieldPermutation;
    }
    
    public void reset(ITupleReference sourceTuple) {
        this.sourceTuple = sourceTuple;
    }

    @Override
    public int getFieldCount() {
        return fieldPermutation.length;
    }

    @Override
    public byte[] getFieldData(int fIdx) {
        return sourceTuple.getFieldData(fieldPermutation[fIdx]);
    }

    @Override
    public int getFieldStart(int fIdx) {
        return sourceTuple.getFieldStart(fieldPermutation[fIdx]);
    }

    @Override
    public int getFieldLength(int fIdx) {
        return sourceTuple.getFieldLength(fieldPermutation[fIdx]);
    }
}
