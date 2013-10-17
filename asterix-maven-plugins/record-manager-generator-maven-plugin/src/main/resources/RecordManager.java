/*
 * Copyright 2013 by The Regents of the University of California
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

package edu.uci.ics.asterix.transaction.management.service.locking;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import edu.uci.ics.asterix.transaction.management.service.locking.AllocInfo;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionManagementConstants.LockManagerConstants.LockMode;

public class @E@RecordManager {

    public static final int SHRINK_TIMER_THRESHOLD = 120000; //2min
    public static final boolean TRACK_ALLOC = @DEBUG@;
    
    static final int NO_SLOTS = 10;
    static final int NEXT_FREE_SLOT_OFFSET = 0;

    @CONSTS@

    private ArrayList<Buffer> buffers;
    private int current;
    private int occupiedSlots;
    private long shrinkTimer;
    private boolean isShrinkTimerOn;

    int allocCounter;
    
    public @E@RecordManager() {
        buffers = new ArrayList<Buffer>();
        buffers.add(new Buffer());
        current = 0;
        
        allocCounter = 0;
    }
    
    synchronized int allocate() {
        if (buffers.get(current).isFull()) {
            int size = buffers.size();
            boolean needNewBuffer = true;
            for (int i = 0; i < size; i++) {
                Buffer buffer = buffers.get(i);
                if (! buffer.isInitialized()) {
                    buffer.initialize();
                    current = i;
                    needNewBuffer = false;
                    break;
                }
            }

            if (needNewBuffer) {
                buffers.add(new Buffer());
                current = buffers.size() - 1;
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
                if (System.currentTimeMillis() - shrinkTimer >= SHRINK_TIMER_THRESHOLD) {
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
        StringBuilder sb = new StringBuilder();
        append(sb);
        return sb.toString();
    }

    static class Buffer {
        private ByteBuffer bb;
        private int freeSlotNum;
        private int occupiedSlots = -1; //-1 represents 'deinitialized' state.
        
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
            
            if (TRACK_ALLOC) {
                allocList = new ArrayList<AllocInfo>(NO_SLOTS);
                for (int i = 0; i < NO_SLOTS; ++i) {
                    allocList.add(new AllocInfo());
                }
            }
        }
        
        public void deinitialize() {
            bb = null;
            occupiedSlots = -1;
        }
        
        public boolean isInitialized() {
            return occupiedSlots >= 0;
        }

        public boolean isFull() {
            return occupiedSlots == NO_SLOTS;
        }

        public boolean isEmpty() {
            return occupiedSlots == 0;
        }
        
        public int allocate() {
            int slotNum = freeSlotNum;
            freeSlotNum = getNextFreeSlot(slotNum);
            @INIT_SLOT@
            occupiedSlots++;
            if (TRACK_ALLOC) allocList.get(slotNum).alloc();
            return slotNum;
        }
    
        public void deallocate(int slotNum) {
            @INIT_SLOT@
            setNextFreeSlot(slotNum, freeSlotNum);
            freeSlotNum = slotNum;
            occupiedSlots--;
            if (TRACK_ALLOC) allocList.get(slotNum).free();
        }

        public int getNextFreeSlot(int slotNum) {
            return bb.getInt(slotNum * ITEM_SIZE + NEXT_FREE_SLOT_OFFSET);
        }    
            
        public void setNextFreeSlot(int slotNum, int nextFreeSlot) {
            bb.putInt(slotNum * ITEM_SIZE + NEXT_FREE_SLOT_OFFSET, nextFreeSlot);
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
        
        private void checkSlot(int slotNum) {
            if (! TRACK_ALLOC) {
                return;
            }
            final int itemOffset = (slotNum % NO_SLOTS) * ITEM_SIZE;
            // @CHECK_SLOT@
        }
    }
    
}
