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

package edu.uci.ics.asterix.transaction.management.service.locking;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;

/**
 * LockWaiterManager manages LockWaiter objects array.
 * The array grows when the slots are overflowed.
 * Also, the array shrinks according to the following shrink policy
 * : Shrink when the resource under-utilization lasts for a certain threshold time.
 * 
 * @author kisskys
 */
public class LockWaiterManager {

    public static final int SHRINK_TIMER_THRESHOLD = 120000; //2min

    private ArrayList<ChildLockWaiterArrayManager> pArray; //parent array
    private int allocChild; //used to allocate the next free LockWaiter object.
    private long shrinkTimer;
    private boolean isShrinkTimerOn;
    private int occupiedSlots;

//    ////////////////////////////////////////////////
//    // begin of unit test
//    ////////////////////////////////////////////////
//
//    public static final int SHRINK_TIMER_THRESHOLD = 0; //for unit test
//
//    /**
//     * @param args
//     */
//    public static void main(String[] args) {
//        final int DataSize = 5000;
//
//        int i, j;
//        int slots = ChildLockWaiterArrayManager.NUM_OF_SLOTS;
//        int data[] = new int[DataSize];
//        LockWaiterManager lwMgr = new LockWaiterManager();
//
//        //allocate: 50
//        System.out.println("allocate: 50");
//        for (i = 0; i < 5; i++) {
//            for (j = i * slots; j < i * slots + slots; j++) {
//                data[j] = lwMgr.allocate();
//            }
//
//            System.out.println(lwMgr.prettyPrint());
//        }
//
//        //deallocate from the last child to the first child
//        System.out.println("deallocate from the last child to the first child");
//        for (i = 4; i >= 0; i--) {
//            for (j = i * slots + slots - 1; j >= i * slots; j--) {
//                lwMgr.deallocate(data[j]);
//            }
//            System.out.println(lwMgr.prettyPrint());
//        }
//
//        //allocate: 50
//        System.out.println("allocate: 50");
//        for (i = 0; i < 5; i++) {
//            for (j = i * slots; j < i * slots + slots; j++) {
//                data[j] = lwMgr.allocate();
//            }
//
//            System.out.println(lwMgr.prettyPrint());
//        }
//
//        //deallocate from the first child to last child
//        System.out.println("deallocate from the first child to last child");
//        for (i = 0; i < 5; i++) {
//            for (j = i * slots; j < i * slots + slots; j++) {
//                lwMgr.deallocate(data[j]);
//            }
//
//            System.out.println(lwMgr.prettyPrint());
//        }
//
//        //allocate: 50
//        System.out.println("allocate: 50");
//        for (i = 0; i < 5; i++) {
//            for (j = i * slots; j < i * slots + slots; j++) {
//                data[j] = lwMgr.allocate();
//            }
//
//            System.out.println(lwMgr.prettyPrint());
//        }
//
//        //deallocate from the first child to 4th child
//        System.out.println("deallocate from the first child to 4th child");
//        for (i = 0; i < 4; i++) {
//            for (j = i * slots; j < i * slots + slots; j++) {
//                lwMgr.deallocate(data[j]);
//            }
//
//            System.out.println(lwMgr.prettyPrint());
//        }
//
//        //allocate: 40
//        System.out.println("allocate: 40");
//        for (i = 0; i < 4; i++) {
//            for (j = i * slots; j < i * slots + slots; j++) {
//                data[j] = lwMgr.allocate();
//            }
//
//            System.out.println(lwMgr.prettyPrint());
//        }
//    }
//
//    ////////////////////////////////////////////////
//    // end of unit test
//    ////////////////////////////////////////////////

    public LockWaiterManager() {
        pArray = new ArrayList<ChildLockWaiterArrayManager>();
        pArray.add(new ChildLockWaiterArrayManager());
        allocChild = 0;
        occupiedSlots = 0;
        isShrinkTimerOn = false;
    }

    public int allocate() {
        if (pArray.get(allocChild).isFull()) {
            int size = pArray.size();
            boolean bAlloc = false;
            ChildLockWaiterArrayManager child;

            //find a deinitialized child and initialize it
            for (int i = 0; i < size; i++) {
                child = pArray.get(i);
                if (child.isDeinitialized()) {
                    child.initialize();
                    allocChild = i;
                    bAlloc = true;
                    break;
                }
            }

            //allocate new child when there is no deinitialized child
            if (!bAlloc) {
                pArray.add(new ChildLockWaiterArrayManager());
                allocChild = pArray.size() - 1;
            }
        }
        occupiedSlots++;
        return pArray.get(allocChild).allocate() + allocChild * ChildLockWaiterArrayManager.NUM_OF_SLOTS;
    }

    void deallocate(int slotNum) {
        pArray.get(slotNum / ChildLockWaiterArrayManager.NUM_OF_SLOTS).deallocate(
                slotNum % ChildLockWaiterArrayManager.NUM_OF_SLOTS);
        occupiedSlots--;

        if (needShrink()) {
            shrink();
        }
    }

    /**
     * Shrink policy:
     * Shrink when the resource under-utilization lasts for a certain amount of time.
     * TODO Need to figure out which of the policies is better
     * case1.
     * pArray status : O x x x x x O (O is initialized, x is deinitialized)
     * In the above status, 'CURRENT' needShrink() returns 'TRUE'
     * even if there is nothing to shrink or deallocate.
     * It doesn't distinguish the deinitialized children from initialized children
     * by calculating totalNumOfSlots = pArray.size() * ChildLockWaiterArrayManager.NUM_OF_SLOTS.
     * In other words, it doesn't subtract the deinitialized children's slots.
     * case2.
     * pArray status : O O x x x x x
     * However, in the above case, if we subtract the deinitialized children's slots,
     * needShrink() will return false even if we shrink the pArray at this case.
     * 
     * @return
     */
    private boolean needShrink() {
        int size = pArray.size();
        int usedSlots = occupiedSlots;
        if (usedSlots == 0) {
            usedSlots = 1;
        }

        if (size > 1 && size * ChildLockWaiterArrayManager.NUM_OF_SLOTS / usedSlots >= 3) {
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
     * deinitialize(:deallocates array of LockWaiter objects in a child) Children(s) or
     * shrink pArray according to the deinitialized children's contiguity status.
     * It doesn't deinitialize or shrink more than half of children at a time.
     */
    private void shrink() {
        int i;
        int removeCount = 0;
        int size = pArray.size();
        int maxDecreaseCount = size / 2;
        ChildLockWaiterArrayManager child;

        //The first buffer never be deinitialized.
        for (i = 1; i < size; i++) {
            if (pArray.get(i).isEmpty()) {
                pArray.get(i).deinitialize();
            }
        }

        //remove the empty buffers from the end
        for (i = size - 1; i >= 1; i--) {
            child = pArray.get(i);
            if (child.isDeinitialized()) {
                pArray.remove(i);
                if (++removeCount == maxDecreaseCount) {
                    break;
                }
            } else {
                break;
            }
        }
        
        //reset allocChild to the first buffer
        allocChild = 0;

        isShrinkTimerOn = false;
    }

    public String prettyPrint() {
        StringBuilder s = new StringBuilder("\n########### LockWaiterManager Status #############\n");
        int size = pArray.size();
        ChildLockWaiterArrayManager child;

        for (int i = 0; i < size; i++) {
            child = pArray.get(i);
            if (child.isDeinitialized()) {
                continue;
            }
            s.append("child[" + i + "]");
            s.append(child.prettyPrint());
        }
        return s.toString();
    }
    
    public void coreDump(OutputStream os) {
        StringBuilder sb = new StringBuilder("\n########### LockWaiterManager Status #############\n");
        int size = pArray.size();
        ChildLockWaiterArrayManager child;

        sb.append("Number of Child: " + size + "\n"); 
        for (int i = 0; i < size; i++) {
            try {
                child = pArray.get(i);
                sb.append("child[" + i + "]");
                sb.append(child.prettyPrint());
                
                os.write(sb.toString().getBytes());
            } catch (IOException e) {
                //ignore IOException
            }
            sb = new StringBuilder();
        }
    }
    
    public int getShrinkTimerThreshold() {
        return SHRINK_TIMER_THRESHOLD;
    }
    
    public LockWaiter getLockWaiter(int slotNum) {
        return pArray.get(slotNum / ChildLockWaiterArrayManager.NUM_OF_SLOTS).getLockWaiter(
                slotNum % ChildLockWaiterArrayManager.NUM_OF_SLOTS);
    }
}

class ChildLockWaiterArrayManager {
    public static final int NUM_OF_SLOTS = 100; //number of LockWaiter objects in 'childArray'.
//    public static final int NUM_OF_SLOTS = 10; //for unit test 

    private int freeSlotNum;
    private int occupiedSlots; //-1 represents 'deinitialized' state.
    LockWaiter childArray[];//childArray

    public ChildLockWaiterArrayManager() {
        initialize();
    }

    public void initialize() {
        this.childArray = new LockWaiter[NUM_OF_SLOTS];
        this.freeSlotNum = 0;
        this.occupiedSlots = 0;

        for (int i = 0; i < NUM_OF_SLOTS - 1; i++) {
            childArray[i] = new LockWaiter();
            childArray[i].setNextFreeSlot(i + 1);
        }
        childArray[NUM_OF_SLOTS - 1] = new LockWaiter();
        childArray[NUM_OF_SLOTS - 1].setNextFreeSlot(-1); //-1 represents EOL(end of link)
    }

    public LockWaiter getLockWaiter(int slotNum) {
        return childArray[slotNum];
    }

    public int allocate() {
        int currentSlot = freeSlotNum;
        freeSlotNum = childArray[currentSlot].getNextFreeSlot();
        childArray[currentSlot].setWait(true);
        childArray[currentSlot].setVictim(false);
        childArray[currentSlot].setWaiterCount((byte)0);
        childArray[currentSlot].setNextWaiterObjId(-1);
        childArray[currentSlot].setNextWaitingResourceObjId(-1);
        childArray[currentSlot].setBeginWaitTime(-1l);
        occupiedSlots++;
        if (LockManager.IS_DEBUG_MODE) {
            System.out.println(Thread.currentThread().getName()+"  Alloc LockWaiterId("+currentSlot+")");
        }
        return currentSlot;
    }

    public void deallocate(int slotNum) {
        childArray[slotNum].setNextFreeSlot(freeSlotNum);
        freeSlotNum = slotNum;
        occupiedSlots--;
        if (LockManager.IS_DEBUG_MODE) {
            System.out.println(Thread.currentThread().getName()+"  Dealloc LockWaiterId("+slotNum+")");
        }
    }

    public void deinitialize() {
        childArray = null;
        occupiedSlots = -1;
    }

    public boolean isDeinitialized() {
        return occupiedSlots == -1;
    }

    public boolean isFull() {
        return occupiedSlots == NUM_OF_SLOTS;
    }

    public boolean isEmpty() {
        return occupiedSlots == 0;
    }

    public int getNumOfOccupiedSlots() {
        return occupiedSlots;
    }

    public int getFreeSlotNum() {
        return freeSlotNum;
    }
    
    public String prettyPrint() {
        LockWaiter waiter;
        StringBuilder sb = new StringBuilder();
        sb.append("\n\toccupiedSlots:" + getNumOfOccupiedSlots());
        sb.append("\n\tfreeSlotNum:" + getFreeSlotNum() + "\n");
        for (int j = 0; j < ChildLockWaiterArrayManager.NUM_OF_SLOTS; j++) {
            waiter = getLockWaiter(j);
            sb.append(j).append(": ");
            sb.append("\t" + waiter.getEntityInfoSlot());
            sb.append("\t" + waiter.needWait());
            sb.append("\t" + waiter.isVictim());
            sb.append("\n");
        }
        return sb.toString();
    }
}