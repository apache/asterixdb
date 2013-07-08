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
package edu.uci.ics.hyracks.dataflow.common.comm.io;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;

public class FrameTuplePairComparator {
    private final int[] keys0;
    private final int[] keys1;
    private final IBinaryComparator[] comparators;

    public FrameTuplePairComparator(int[] keys0, int[] keys1, IBinaryComparator[] comparators) {
        this.keys0 = keys0;
        this.keys1 = keys1;
        this.comparators = comparators;
    }

    public int compare(FrameTupleAccessor accessor0, int tIndex0, FrameTupleAccessor accessor1, int tIndex1) {
        int tStart0 = accessor0.getTupleStartOffset(tIndex0);
        int fStartOffset0 = accessor0.getFieldSlotsLength() + tStart0;

        int tStart1 = accessor1.getTupleStartOffset(tIndex1);
        int fStartOffset1 = accessor1.getFieldSlotsLength() + tStart1;

        for (int i = 0; i < keys0.length; ++i) {
            int fIdx0 = keys0[i];
            int fStart0 = accessor0.getFieldStartOffset(tIndex0, fIdx0);
            int fEnd0 = accessor0.getFieldEndOffset(tIndex0, fIdx0);
            int fLen0 = fEnd0 - fStart0;

            int fIdx1 = keys1[i];
            int fStart1 = accessor1.getFieldStartOffset(tIndex1, fIdx1);
            int fEnd1 = accessor1.getFieldEndOffset(tIndex1, fIdx1);
            int fLen1 = fEnd1 - fStart1;

            int c = comparators[i].compare(accessor0.getBuffer().array(), fStart0 + fStartOffset0, fLen0, accessor1
                    .getBuffer().array(), fStart1 + fStartOffset1, fLen1);
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }
}