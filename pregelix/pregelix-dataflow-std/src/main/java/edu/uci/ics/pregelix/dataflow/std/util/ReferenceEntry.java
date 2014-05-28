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
package edu.uci.ics.pregelix.dataflow.std.util;

import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.pregelix.dataflow.std.sort.RawNormalizedKeyComputer;

public final class ReferenceEntry {
    private final int runid;
    private FrameTupleAccessor acccessor;
    private int tupleIndex;
    private int[] tPointers;
    private boolean exhausted = false;

    public ReferenceEntry(int runid, FrameTupleAccessor fta, int tupleIndex, int[] keyFields,
            RawNormalizedKeyComputer nmkComputer) {
        super();
        this.runid = runid;
        this.acccessor = fta;
        this.tPointers = new int[2 + 2 * keyFields.length];
        if (fta != null) {
            initTPointer(fta, tupleIndex, keyFields, nmkComputer);
        }
    }

    public int getRunid() {
        return runid;
    }

    public FrameTupleAccessor getAccessor() {
        return acccessor;
    }

    public void setAccessor(FrameTupleAccessor fta) {
        this.acccessor = fta;
    }

    public int[] getTPointers() {
        return tPointers;
    }

    public int getTupleIndex() {
        return tupleIndex;
    }

    public int getNormalizedKey() {
        return tPointers[0];
    }

    public int getNormalizedKey4() {
        return tPointers[1];
    }

    public void setTupleIndex(int tupleIndex, int[] keyFields, RawNormalizedKeyComputer nmkComputer) {
        initTPointer(acccessor, tupleIndex, keyFields, nmkComputer);
    }

    public void setExhausted() {
        this.exhausted = true;
    }

    public boolean isExhausted() {
        return this.exhausted;
    }

    private void initTPointer(FrameTupleAccessor fta, int tupleIndex, int[] keyFields,
            RawNormalizedKeyComputer nmkComputer) {
        this.tupleIndex = tupleIndex;
        byte[] b1 = fta.getBuffer().array();
        for (int f = 0; f < keyFields.length; ++f) {
            int fIdx = keyFields[f];
            tPointers[2 * f + 2] = fta.getTupleStartOffset(tupleIndex) + fta.getFieldSlotsLength()
                    + fta.getFieldStartOffset(tupleIndex, fIdx);
            tPointers[2 * f + 3] = fta.getFieldEndOffset(tupleIndex, fIdx) - fta.getFieldStartOffset(tupleIndex, fIdx);
            if (f == 0) {
                tPointers[0] = nmkComputer == null ? 0 : nmkComputer.normalize(b1, tPointers[2], tPointers[3]);
                tPointers[1] = nmkComputer == null ? 0 : nmkComputer.normalize4(b1, tPointers[2], tPointers[3]);
            }
        }
    }
}