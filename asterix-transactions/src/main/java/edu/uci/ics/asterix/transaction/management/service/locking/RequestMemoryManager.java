package edu.uci.ics.asterix.transaction.management.service.locking;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionManagementConstants.LockManagerConstants.LockMode;

public class RequestMemoryManager {

    public static final int SHRINK_TIMER_THRESHOLD = 120000; //2min

    static final int NO_SLOTS = 10;
    static final int NEXT_FREE_SLOT_OFFSET = 0;

    public static int ITEM_SIZE = 24;
    public static int RESOURCE_ID_OFF = 0;
    public static int LOCK_MODE_OFF = 4;
    public static int JOB_ID_OFF = 8;
    public static int PREV_JOB_REQUEST_OFF = 12;
    public static int NEXT_JOB_REQUEST_OFF = 16;
    public static int NEXT_REQUEST_OFF = 20;


    private ArrayList<Buffer> buffers;
    private int current;
    private int occupiedSlots;
    private long shrinkTimer;
    private boolean isShrinkTimerOn;
    
    public RequestMemoryManager() {
        buffers = new ArrayList<Buffer>();
        buffers.add(new Buffer());
        current = 0;
    }
    
    public int allocate() {
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

    void deallocate(int slotNum) {
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

    public int getResourceId(int slotNum) {
        final ByteBuffer b = buffers.get(slotNum / NO_SLOTS).bb;
        return b.getInt((slotNum % NO_SLOTS) * ITEM_SIZE + RESOURCE_ID_OFF);
    }

    public void setResourceId(int slotNum, int value) {
        final ByteBuffer b = buffers.get(slotNum / NO_SLOTS).bb;
        b.putInt((slotNum % NO_SLOTS) * ITEM_SIZE + RESOURCE_ID_OFF, value);
    }

    public int getLockMode(int slotNum) {
        final ByteBuffer b = buffers.get(slotNum / NO_SLOTS).bb;
        return b.getInt((slotNum % NO_SLOTS) * ITEM_SIZE + LOCK_MODE_OFF);
    }

    public void setLockMode(int slotNum, int value) {
        final ByteBuffer b = buffers.get(slotNum / NO_SLOTS).bb;
        b.putInt((slotNum % NO_SLOTS) * ITEM_SIZE + LOCK_MODE_OFF, value);
    }

    public int getJobId(int slotNum) {
        final ByteBuffer b = buffers.get(slotNum / NO_SLOTS).bb;
        return b.getInt((slotNum % NO_SLOTS) * ITEM_SIZE + JOB_ID_OFF);
    }

    public void setJobId(int slotNum, int value) {
        final ByteBuffer b = buffers.get(slotNum / NO_SLOTS).bb;
        b.putInt((slotNum % NO_SLOTS) * ITEM_SIZE + JOB_ID_OFF, value);
    }

    public int getPrevJobRequest(int slotNum) {
        final ByteBuffer b = buffers.get(slotNum / NO_SLOTS).bb;
        return b.getInt((slotNum % NO_SLOTS) * ITEM_SIZE + PREV_JOB_REQUEST_OFF);
    }

    public void setPrevJobRequest(int slotNum, int value) {
        final ByteBuffer b = buffers.get(slotNum / NO_SLOTS).bb;
        b.putInt((slotNum % NO_SLOTS) * ITEM_SIZE + PREV_JOB_REQUEST_OFF, value);
    }

    public int getNextJobRequest(int slotNum) {
        final ByteBuffer b = buffers.get(slotNum / NO_SLOTS).bb;
        return b.getInt((slotNum % NO_SLOTS) * ITEM_SIZE + NEXT_JOB_REQUEST_OFF);
    }

    public void setNextJobRequest(int slotNum, int value) {
        final ByteBuffer b = buffers.get(slotNum / NO_SLOTS).bb;
        b.putInt((slotNum % NO_SLOTS) * ITEM_SIZE + NEXT_JOB_REQUEST_OFF, value);
    }

    public int getNextRequest(int slotNum) {
        final ByteBuffer b = buffers.get(slotNum / NO_SLOTS).bb;
        return b.getInt((slotNum % NO_SLOTS) * ITEM_SIZE + NEXT_REQUEST_OFF);
    }

    public void setNextRequest(int slotNum, int value) {
        final ByteBuffer b = buffers.get(slotNum / NO_SLOTS).bb;
        b.putInt((slotNum % NO_SLOTS) * ITEM_SIZE + NEXT_REQUEST_OFF, value);
    }

    

    static class Buffer {
        private ByteBuffer bb;
        private int freeSlotNum;
        private int occupiedSlots = -1; //-1 represents 'deinitialized' state.
        
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
            int currentSlot = freeSlotNum;
            freeSlotNum = getNextFreeSlot(currentSlot);
            occupiedSlots++;
            if (LockManager.IS_DEBUG_MODE) {
                System.out.println(Thread.currentThread().getName()
                                   + " allocate: " + currentSlot);
            }
            return currentSlot;
        }
    
        public void deallocate(int slotNum) {
            setNextFreeSlot(slotNum, freeSlotNum);
            freeSlotNum = slotNum;
            occupiedSlots--;
            if (LockManager.IS_DEBUG_MODE) {
                System.out.println(Thread.currentThread().getName()
                                   + " deallocate: " + slotNum);
            }
        }

        public int getNextFreeSlot(int slotNum) {
            return bb.getInt(slotNum * ITEM_SIZE + NEXT_FREE_SLOT_OFFSET);
        }    
            
        public void setNextFreeSlot(int slotNum, int nextFreeSlot) {
            bb.putInt(slotNum * ITEM_SIZE + NEXT_FREE_SLOT_OFFSET, nextFreeSlot);
        }
    }
    
}

