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

package @PACKAGE@;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class @E@RecordManager {

    public static final boolean CHECK_SLOTS = @DEBUG@;
    public static final boolean TRACK_ALLOC_LOC = @DEBUG@;

    static final int NO_SLOTS = 1000;

    @CONSTS@

    private final long txnShrinkTimer;
    private long shrinkTimer;
    private ArrayList<Buffer> buffers;
    private int current;
    private int occupiedSlots;
    private boolean isShrinkTimerOn;

    int allocCounter;

    public @E@RecordManager(long txnShrinkTimer) {
        this.txnShrinkTimer = txnShrinkTimer;
        buffers = new ArrayList<Buffer>();
        buffers.add(new Buffer());
        current = 0;

        allocCounter = 0;
    }

    enum SlotSource {
        NON_FULL,
        UNINITIALIZED,
        NEW
    }

    synchronized int allocate() {
        if (buffers.get(current).isFull()) {
            final int size = buffers.size();
            final int start = current + 1;
            SlotSource source = SlotSource.NEW;
            for (int j = start; j < start + size; ++j) {
                // If we find a buffer with space, we use it. Otherwise we
                // remember the first uninitialized one and use that one.
                final int i = j % size;
                final Buffer buffer = buffers.get(i);
                if (buffer.isInitialized() && ! buffer.isFull()) {
                    source = SlotSource.NON_FULL;
                    current = i;
                    break;
                } else if (! buffer.isInitialized() && source == SlotSource.NEW) {
                    source = SlotSource.UNINITIALIZED;
                    current = i;
                }
            }

            switch (source) {
                case NEW:
                    buffers.add(new Buffer());
                    current = buffers.size() - 1;
                    break;
                case UNINITIALIZED:
                    buffers.get(current).initialize();
                case NON_FULL:
                    break;
            }
        }
        ++occupiedSlots;
        return buffers.get(current).allocate() + current * NO_SLOTS;
    }

    synchronized void deallocate(int slotNum) {
        buffers.get(slotNum / NO_SLOTS).deallocate(slotNum % NO_SLOTS);
        --occupiedSlots;

        if (needShrink()) {
            shrink();
        }
    }

    /**
     * Shrink policy:
     * Shrink when the resource under-utilization lasts for a certain amount of time.
     * TODO Need to figure out which of the policies is better
     * case1.
     * buffers status : O x x x x x O (O is initialized, x is deinitialized)
     * In the above status, 'CURRENT' needShrink() returns 'TRUE'
     * even if there is nothing to shrink or deallocate.
     * It doesn't distinguish the deinitialized children from initialized children
     * by calculating totalNumOfSlots = buffers.size() * ChildEntityLockInfoArrayManager.NUM_OF_SLOTS.
     * In other words, it doesn't subtract the deinitialized children's slots.
     * case2.
     * buffers status : O O x x x x x
     * However, in the above case, if we subtract the deinitialized children's slots,
     * needShrink() will return false even if we shrink the buffers at this case.
     *
     * @return
     */
    private boolean needShrink() {
        int size = buffers.size();
        int usedSlots = occupiedSlots;
        if (usedSlots == 0) {
            usedSlots = 1;
        }

        if (size > 1 && size * NO_SLOTS / usedSlots >= 3) {
            if (isShrinkTimerOn) {
                if (System.currentTimeMillis() - shrinkTimer >= txnShrinkTimer) {
                    isShrinkTimerOn = false;
                    return true;
                }
            } else {
                //turn on timer
                isShrinkTimerOn = true;
                shrinkTimer = System.currentTimeMillis();
            }
        } else {
            //turn off timer
            isShrinkTimerOn = false;
        }

        return false;
    }

    /**
     * Shrink() may
     * deinitialize(:deallocates ByteBuffer of child) Children(s) or
     * shrink buffers according to the deinitialized children's contiguity status.
     * It doesn't deinitialze or shrink more than half of children at a time.
     */
    private void shrink() {
        int i;
        int removeCount = 0;
        int size = buffers.size();
        int maxDecreaseCount = size / 2;
        Buffer buffer;

        //The first buffer never be deinitialized.
        for (i = 1; i < size; i++) {
            if (buffers.get(i).isEmpty()) {
                buffers.get(i).deinitialize();
            }
        }

        //remove the empty buffers from the end
        for (i = size - 1; i >= 1; i--) {
            buffer = buffers.get(i);
            if (! buffer.isInitialized()) {
                buffers.remove(i);
                if (++removeCount == maxDecreaseCount) {
                    break;
                }
            } else {
                break;
            }
        }

        //reset allocChild to the first buffer
        current = 0;

        isShrinkTimerOn = false;
    }

    @METHODS@

    public AllocInfo getAllocInfo(int slotNum) {
        final Buffer buf = buffers.get(slotNum / NO_SLOTS);
        if (buf.allocList == null) {
            return null;
        } else {
            return buf.allocList.get(slotNum % NO_SLOTS);
        }
    }

    StringBuilder append(StringBuilder sb) {
        sb.append("+++ current: ")
          .append(current)
          .append(" no occupied slots: ")
          .append(occupiedSlots)
          .append(" +++\n");
        for (int i = 0; i < buffers.size(); ++i) {
            buffers.get(i).append(sb);
            sb.append("\n");
        }
        return sb;
    }

    public String toString() {
        return append(new StringBuilder()).toString();
    }

    public RecordManagerStats addTo(RecordManagerStats s) {
        final int size = buffers.size();
        s.buffers += size;
        s.slots += size * NO_SLOTS;
        s.size += size * NO_SLOTS * ITEM_SIZE;
        for (int i = 0; i < size; ++i) {
            buffers.get(i).addTo(s);
        }
        return s;
    }

    static class Buffer {
        private ByteBuffer bb = null; // null represents 'deinitialized' state.
        private int freeSlotNum;
        private int occupiedSlots;

        ArrayList<AllocInfo> allocList;

        Buffer() {
            initialize();
        }

        void initialize() {
            bb = ByteBuffer.allocate(NO_SLOTS * ITEM_SIZE);
            freeSlotNum = 0;
            occupiedSlots = 0;

            for (int i = 0; i < NO_SLOTS - 1; i++) {
                setNextFreeSlot(i, i + 1);
            }
            setNextFreeSlot(NO_SLOTS - 1, -1); //-1 represents EOL(end of link)

            if (TRACK_ALLOC_LOC) {
                allocList = new ArrayList<AllocInfo>(NO_SLOTS);
                for (int i = 0; i < NO_SLOTS; ++i) {
                    allocList.add(new AllocInfo());
                }
            }
        }

        public void deinitialize() {
            if (TRACK_ALLOC_LOC) allocList = null;
            bb = null;
        }

        public boolean isInitialized() {
            return bb != null;
        }

        public boolean isFull() {
            return freeSlotNum == -1;
        }

        public boolean isEmpty() {
            return occupiedSlots == 0;
        }

        public int allocate() {
            int slotNum = freeSlotNum;
            freeSlotNum = getNextFreeSlot(slotNum);
            @INIT_SLOT@
            occupiedSlots++;
            if (TRACK_ALLOC_LOC) allocList.get(slotNum).alloc();
            return slotNum;
        }

        public void deallocate(int slotNum) {
            @INIT_SLOT@
            setNextFreeSlot(slotNum, freeSlotNum);
            freeSlotNum = slotNum;
            occupiedSlots--;
            if (TRACK_ALLOC_LOC) allocList.get(slotNum).free();
        }

        public int getNextFreeSlot(int slotNum) {
            return bb.getInt(slotNum * ITEM_SIZE + NEXT_FREE_SLOT_OFF);
        }

        public void setNextFreeSlot(int slotNum, int nextFreeSlot) {
            bb.putInt(slotNum * ITEM_SIZE + NEXT_FREE_SLOT_OFF, nextFreeSlot);
        }

        StringBuilder append(StringBuilder sb) {
            sb.append("++ free slot: ")
              .append(freeSlotNum)
              .append(" no occupied slots: ")
              .append(occupiedSlots)
              .append(" ++\n");
            @PRINT_BUFFER@
            return sb;
        }

        public String toString() {
            return append(new StringBuilder()).toString();
        }

        public void addTo(RecordManagerStats s) {
            if (isInitialized()) {
                s.items += occupiedSlots;
            }
        }

        private void checkSlot(int slotNum) {
            if (! CHECK_SLOTS) {
                return;
            }
            final int itemOffset = (slotNum % NO_SLOTS) * ITEM_SIZE;
            // @CHECK_SLOT@
        }
    }

}
