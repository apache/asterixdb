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

package edu.uci.ics.hyracks.storage.am.rtree.frames;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProvider;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.rtree.api.IRTreeLeafFrame;

public class RTreeNSMLeafFrame extends RTreeNSMFrame implements IRTreeLeafFrame {

    public RTreeNSMLeafFrame(ITreeIndexTupleWriter tupleWriter, IPrimitiveValueProvider[] keyValueProviders,
            RTreePolicyType rtreePolicyType) {
        super(tupleWriter, keyValueProviders, rtreePolicyType);
    }

    @Override
    public int getBytesRequriedToWriteTuple(ITupleReference tuple) {
        return tupleWriter.bytesRequired(tuple) + slotManager.getSlotSize();
    }

    @Override
    public ITreeIndexTupleReference createTupleReference() {
        return tupleWriter.createTupleReference();
    }

    @Override
    public int findTupleIndex(ITupleReference tuple, MultiComparator cmp) {
        return slotManager.findTupleIndex(tuple, frameTuple, cmp, null, null);
    }

    @Override
    public boolean intersect(ITupleReference tuple, int tupleIndex, MultiComparator cmp) {
        frameTuple.resetByTupleIndex(this, tupleIndex);
        int maxFieldPos = cmp.getKeyFieldCount() / 2;
        for (int i = 0; i < maxFieldPos; i++) {
            int j = maxFieldPos + i;
            int c = cmp.getComparators()[i].compare(tuple.getFieldData(i), tuple.getFieldStart(i),
                    tuple.getFieldLength(i), frameTuple.getFieldData(j), frameTuple.getFieldStart(j),
                    frameTuple.getFieldLength(j));
            if (c > 0) {
                return false;
            }
            c = cmp.getComparators()[i].compare(tuple.getFieldData(j), tuple.getFieldStart(j), tuple.getFieldLength(j),
                    frameTuple.getFieldData(i), frameTuple.getFieldStart(i), frameTuple.getFieldLength(i));

            if (c < 0) {
                return false;
            }
        }
        return true;
    }

    public int getTupleSize(ITupleReference tuple) {
        return tupleWriter.bytesRequired(tuple);
    }

    @Override
    public void insert(ITupleReference tuple, int tupleIndex) {
        slotManager.insertSlot(-1, buf.getInt(freeSpaceOff));
        int bytesWritten = tupleWriter.writeTuple(tuple, buf.array(), buf.getInt(freeSpaceOff));

        buf.putInt(tupleCountOff, buf.getInt(tupleCountOff) + 1);
        buf.putInt(freeSpaceOff, buf.getInt(freeSpaceOff) + bytesWritten);
        buf.putInt(totalFreeSpaceOff, buf.getInt(totalFreeSpaceOff) - bytesWritten - slotManager.getSlotSize());
    }

    @Override
    public void delete(int tupleIndex, MultiComparator cmp) {
        int slotOff = slotManager.getSlotOff(tupleIndex);

        int tupleOff = slotManager.getTupleOff(slotOff);
        frameTuple.resetByTupleOffset(buf, tupleOff);
        int tupleSize = tupleWriter.bytesRequired(frameTuple);

        // perform deletion (we just do a memcpy to overwrite the slot)
        int slotStartOff = slotManager.getSlotEndOff();
        int length = slotOff - slotStartOff;
        System.arraycopy(buf.array(), slotStartOff, buf.array(), slotStartOff + slotManager.getSlotSize(), length);

        // maintain space information
        buf.putInt(tupleCountOff, buf.getInt(tupleCountOff) - 1);
        buf.putInt(totalFreeSpaceOff, buf.getInt(totalFreeSpaceOff) + tupleSize + slotManager.getSlotSize());
    }

    @Override
    public int getFieldCount() {
        return frameTuple.getFieldCount();
    }

    public ITupleReference getBeforeTuple(ITupleReference tuple, int targetTupleIndex, MultiComparator cmp) {
        // Examine the tuple index to determine whether it is valid or not.
        if (targetTupleIndex != slotManager.getGreatestKeyIndicator()) {
            // We need to check the key to determine whether it's an insert or an update.
            frameTuple.resetByTupleIndex(this, targetTupleIndex);
            if (cmp.compare(tuple, frameTuple) == 0) {
                // The keys match, it's an update.
                return frameTuple;
            }
        }
        // Either the tuple index is a special indicator, or the keys don't match.
        // In those cases, we are definitely dealing with an insert.
        return null;
    }
}
