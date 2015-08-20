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

package edu.uci.ics.hyracks.storage.am.btree.impls;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.storage.am.btree.api.IPrefixSlotManager;
import edu.uci.ics.hyracks.storage.am.btree.frames.BTreeFieldPrefixNSMLeafFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleReference;

public class FieldPrefixPrefixTupleReference extends TypeAwareTupleReference {

    public FieldPrefixPrefixTupleReference(ITypeTraits[] typeTraits) {
        super(typeTraits);
    }

    // assumes tuple index refers to prefix tuples
    @Override
    public void resetByTupleIndex(ITreeIndexFrame frame, int tupleIndex) {
        BTreeFieldPrefixNSMLeafFrame concreteFrame = (BTreeFieldPrefixNSMLeafFrame) frame;
        IPrefixSlotManager slotManager = concreteFrame.getSlotManager();
        int prefixSlotOff = slotManager.getPrefixSlotOff(tupleIndex);
        int prefixSlot = concreteFrame.getBuffer().getInt(prefixSlotOff);
        setFieldCount(slotManager.decodeFirstSlotField(prefixSlot));
        tupleStartOff = slotManager.decodeSecondSlotField(prefixSlot);
        buf = concreteFrame.getBuffer();
        resetByTupleOffset(buf, tupleStartOff);
    }
}
