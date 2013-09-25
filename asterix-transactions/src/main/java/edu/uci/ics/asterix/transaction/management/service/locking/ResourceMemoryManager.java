package edu.uci.ics.asterix.transaction.management.service.locking;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionManagementConstants.LockManagerConstants.LockMode;

public class ResourceMemoryManager {

    public static final int SHRINK_TIMER_THRESHOLD = 120000; //2min

    static final int NO_SLOTS = 10;
    static final int NEXT_FREE_SLOT_OFFSET = 0;

    public static int ITEM_SIZE = 28;
    public static int LAST_HOLDER_OFF = 0;
    public static int FIRST_WAITER_OFF = 4;
    public static int FIRST_UPGRADER_OFF = 8;
    public static int MAX_MODE_OFF = 12;
    public static int DATASET_ID_OFF = 16;
    public static int PK_HASH_VAL_OFF = 20;
    public static int NEXT_OFF = 24;


    private ArrayList<Buffer> buffers;
    private int current;
    private int occupiedSlots;
    private long shrinkTimer;
    private boolean isShrinkTimerOn;
    
    public ResourceMemoryManager() {
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

    public int getLastHolder(int slotNum) {
        final ByteBuffer b = buffers.get(slotNum / NO_SLOTS).bb;
        return b.getInt((slotNum % NO_SLOTS) * ITEM_SIZE + LAST_HOLDER_OFF);
    }

    public void setLastHolder(int slotNum, int value) {
        final ByteBuffer b = buffers.get(slotNum / NO_SLOTS).bb;
        b.putInt((slotNum % NO_SLOTS) * ITEM_SIZE + LAST_HOLDER_OFF, value);
    }

    public int getFirstWaiter(int slotNum) {
        final ByteBuffer b = buffers.get(slotNum / NO_SLOTS).bb;
        return b.getInt((slotNum % NO_SLOTS) * ITEM_SIZE + FIRST_WAITER_OFF);
    }

    public void setFirstWaiter(int slotNum, int value) {
        final ByteBuffer b = buffers.get(slotNum / NO_SLOTS).bb;
        b.putInt((slotNum % NO_SLOTS) * ITEM_SIZE + FIRST_WAITER_OFF, value);
    }

    public int getFirstUpgrader(int slotNum) {
        final ByteBuffer b = buffers.get(slotNum / NO_SLOTS).bb;
        return b.getInt((slotNum % NO_SLOTS) * ITEM_SIZE + FIRST_UPGRADER_OFF);
    }

    public void setFirstUpgrader(int slotNum, int value) {
        final ByteBuffer b = buffers.get(slotNum / NO_SLOTS).bb;
        b.putInt((slotNum % NO_SLOTS) * ITEM_SIZE + FIRST_UPGRADER_OFF, value);
    }

    public int getMaxMode(int slotNum) {
        final ByteBuffer b = buffers.get(slotNum / NO_SLOTS).bb;
        return b.getInt((slotNum % NO_SLOTS) * ITEM_SIZE + MAX_MODE_OFF);
    }

    public void setMaxMode(int slotNum, int value) {
        final ByteBuffer b = buffers.get(slotNum / NO_SLOTS).bb;
        b.putInt((slotNum % NO_SLOTS) * ITEM_SIZE + MAX_MODE_OFF, value);
    }

    public int getDatasetId(int slotNum) {
        final ByteBuffer b = buffers.get(slotNum / NO_SLOTS).bb;
        return b.getInt((slotNum % NO_SLOTS) * ITEM_SIZE + DATASET_ID_OFF);
    }

    public void setDatasetId(int slotNum, int value) {
        final ByteBuffer b = buffers.get(slotNum / NO_SLOTS).bb;
        b.putInt((slotNum % NO_SLOTS) * ITEM_SIZE + DATASET_ID_OFF, value);
    }

    public int getPkHashVal(int slotNum) {
        final ByteBuffer b = buffers.get(slotNum / NO_SLOTS).bb;
        return b.getInt((slotNum % NO_SLOTS) * ITEM_SIZE + PK_HASH_VAL_OFF);
    }

    public void setPkHashVal(int slotNum, int value) {
        final ByteBuffer b = buffers.get(slotNum / NO_SLOTS).bb;
        b.putInt((slotNum % NO_SLOTS) * ITEM_SIZE + PK_HASH_VAL_OFF, value);
    }

    public int getNext(int slotNum) {
        final ByteBuffer b = buffers.get(slotNum / NO_SLOTS).bb;
        return b.getInt((slotNum % NO_SLOTS) * ITEM_SIZE + NEXT_OFF);
    }

    public void setNext(int slotNum, int value) {
        final ByteBuffer b = buffers.get(slotNum / NO_SLOTS).bb;
        b.putInt((slotNum % NO_SLOTS) * ITEM_SIZE + NEXT_OFF, value);
    }

    
    StringBuffer append(StringBuffer sb) {
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
        StringBuffer sb = new StringBuffer();
        append(sb);
        return sb.toString();
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
            bb.putInt(currentSlot * ITEM_SIZE + LAST_HOLDER_OFF, -1);
            bb.putInt(currentSlot * ITEM_SIZE + FIRST_WAITER_OFF, -1);
            bb.putInt(currentSlot * ITEM_SIZE + FIRST_UPGRADER_OFF, -1);
            bb.putInt(currentSlot * ITEM_SIZE + MAX_MODE_OFF, LockMode.NL);
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

        StringBuffer append(StringBuffer sb) {
            sb.append("++ free slot: ")
              .append(freeSlotNum)
              .append(" no occupied slots: ")
              .append(occupiedSlots)
              .append(" ++\n");
            sb.append("last holder    | ");
            for (int i = 0; i < occupiedSlots; ++i) {
                int value = bb.getInt(i * ITEM_SIZE + LAST_HOLDER_OFF);
                sb.append(String.format("%1$2x", ResourceArenaManager.arenaId(value)));
                sb.append(" ");
                sb.append(String.format("%1$6x", ResourceArenaManager.localId(value)));
                sb.append(" | ");
            }
            sb.append("\n");
            sb.append("first waiter   | ");
            for (int i = 0; i < occupiedSlots; ++i) {
                int value = bb.getInt(i * ITEM_SIZE + FIRST_WAITER_OFF);
                sb.append(String.format("%1$2x", ResourceArenaManager.arenaId(value)));
                sb.append(" ");
                sb.append(String.format("%1$6x", ResourceArenaManager.localId(value)));
                sb.append(" | ");
            }
            sb.append("\n");
            sb.append("first upgrader | ");
            for (int i = 0; i < occupiedSlots; ++i) {
                int value = bb.getInt(i * ITEM_SIZE + FIRST_UPGRADER_OFF);
                sb.append(String.format("%1$2x", ResourceArenaManager.arenaId(value)));
                sb.append(" ");
                sb.append(String.format("%1$6x", ResourceArenaManager.localId(value)));
                sb.append(" | ");
            }
            sb.append("\n");
            sb.append("max mode       | ");
            for (int i = 0; i < occupiedSlots; ++i) {
                int value = bb.getInt(i * ITEM_SIZE + MAX_MODE_OFF);
                sb.append(String.format("%1$2x", ResourceArenaManager.arenaId(value)));
                sb.append(" ");
                sb.append(String.format("%1$6x", ResourceArenaManager.localId(value)));
                sb.append(" | ");
            }
            sb.append("\n");
            sb.append("dataset id     | ");
            for (int i = 0; i < occupiedSlots; ++i) {
                int value = bb.getInt(i * ITEM_SIZE + DATASET_ID_OFF);
                sb.append(String.format("%1$2x", ResourceArenaManager.arenaId(value)));
                sb.append(" ");
                sb.append(String.format("%1$6x", ResourceArenaManager.localId(value)));
                sb.append(" | ");
            }
            sb.append("\n");
            sb.append("pk hash val    | ");
            for (int i = 0; i < occupiedSlots; ++i) {
                int value = bb.getInt(i * ITEM_SIZE + PK_HASH_VAL_OFF);
                sb.append(String.format("%1$2x", ResourceArenaManager.arenaId(value)));
                sb.append(" ");
                sb.append(String.format("%1$6x", ResourceArenaManager.localId(value)));
                sb.append(" | ");
            }
            sb.append("\n");
            sb.append("next           | ");
            for (int i = 0; i < occupiedSlots; ++i) {
                int value = bb.getInt(i * ITEM_SIZE + NEXT_OFF);
                sb.append(String.format("%1$2x", ResourceArenaManager.arenaId(value)));
                sb.append(" ");
                sb.append(String.format("%1$6x", ResourceArenaManager.localId(value)));
                sb.append(" | ");
            }
            sb.append("\n");

            return sb;
        }
        
        public String toString() {
            StringBuffer sb = new StringBuffer();
            append(sb);
            return sb.toString();
        }
    }
    
}

