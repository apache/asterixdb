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
import java.nio.ByteBuffer;
import java.util.ArrayList;

import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionManagementConstants.LockManagerConstants.LockMode;

/**
 * EntityLockInfoManager provides EntityLockInfo arrays backed by ByteBuffer.
 * The array grows when the slots are overflowed.
 * Also, the array shrinks according to the following shrink policy
 * : Shrink when the resource under-utilization lasts for a certain threshold time.
 * 
 * @author kisskys
 */
public class EntityLockInfoManager {

    public static final int SHRINK_TIMER_THRESHOLD = 120000; //2min

    private ArrayList<ChildEntityLockInfoArrayManager> pArray;
    private int allocChild; //used to allocate the next free EntityInfo slot.
    private long shrinkTimer;
    private boolean isShrinkTimerOn;
    private int occupiedSlots;
    private EntityInfoManager entityInfoManager;
    LockWaiterManager lockWaiterManager;

    //        ////////////////////////////////////////////////
    //        // begin of unit test
    //        ////////////////////////////////////////////////
    //    
    //        public static final int SHRINK_TIMER_THRESHOLD = 0; //for unit test
    //    
    //        /**
    //         * @param args
    //         */
    //        public static void main(String[] args) {
    //            final int DataSize = 5000;
    //    
    //            int i, j;
    //            int slots = ChildEntityLockInfoArrayManager.NUM_OF_SLOTS;
    //            int data[] = new int[DataSize];
    //            EntityLockInfoManager eliMgr = new EntityLockInfoManager();
    //    
    //            //allocate: 50
    //            System.out.println("allocate: 50");
    //            for (i = 0; i < 5; i++) {
    //                for (j = i * slots; j < i * slots + slots; j++) {
    //                    data[j] = eliMgr.allocate();
    //                }
    //    
    //                System.out.println(eliMgr.prettyPrint());
    //            }
    //    
    //            //deallocate from the last child to the first child
    //            System.out.println("deallocate from the last child to the first child");
    //            for (i = 4; i >= 0; i--) {
    //                for (j = i * slots + slots - 1; j >= i * slots; j--) {
    //                    eliMgr.deallocate(data[j]);
    //                }
    //                System.out.println(eliMgr.prettyPrint());
    //            }
    //    
    //            //allocate: 50
    //            System.out.println("allocate: 50");
    //            for (i = 0; i < 5; i++) {
    //                for (j = i * slots; j < i * slots + slots; j++) {
    //                    data[j] = eliMgr.allocate();
    //                }
    //    
    //                System.out.println(eliMgr.prettyPrint());
    //            }
    //    
    //            //deallocate from the first child to last child
    //            System.out.println("deallocate from the first child to last child");
    //            for (i = 0; i < 5; i++) {
    //                for (j = i * slots; j < i * slots + slots; j++) {
    //                    eliMgr.deallocate(data[j]);
    //                }
    //    
    //                System.out.println(eliMgr.prettyPrint());
    //            }
    //    
    //            //allocate: 50
    //            System.out.println("allocate: 50");
    //            for (i = 0; i < 5; i++) {
    //                for (j = i * slots; j < i * slots + slots; j++) {
    //                    data[j] = eliMgr.allocate();
    //                }
    //    
    //                System.out.println(eliMgr.prettyPrint());
    //            }
    //    
    //            //deallocate from the first child to 4th child
    //            System.out.println("deallocate from the first child to 4th child");
    //            for (i = 0; i < 4; i++) {
    //                for (j = i * slots; j < i * slots + slots; j++) {
    //                    eliMgr.deallocate(data[j]);
    //                }
    //    
    //                System.out.println(eliMgr.prettyPrint());
    //            }
    //    
    //            //allocate: 40
    //            System.out.println("allocate: 40");
    //            for (i = 0; i < 4; i++) {
    //                for (j = i * slots; j < i * slots + slots; j++) {
    //                    data[j] = eliMgr.allocate();
    //                }
    //    
    //                System.out.println(eliMgr.prettyPrint());
    //            }
    //        }
    //        
    //        ////////////////////////////////////////////////
    //        // end of unit test
    //        ////////////////////////////////////////////////

    public EntityLockInfoManager(EntityInfoManager entityInfoManager, LockWaiterManager lockWaiterManager) {
        pArray = new ArrayList<ChildEntityLockInfoArrayManager>();
        pArray.add(new ChildEntityLockInfoArrayManager());
        allocChild = 0;
        occupiedSlots = 0;
        isShrinkTimerOn = false;
        this.entityInfoManager = entityInfoManager;
        this.lockWaiterManager = lockWaiterManager;
    }

    public int allocate() {
        if (pArray.get(allocChild).isFull()) {
            int size = pArray.size();
            boolean bAlloc = false;
            ChildEntityLockInfoArrayManager child;

            //find a deinitialized child and initialze it
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
                pArray.add(new ChildEntityLockInfoArrayManager());
                allocChild = pArray.size() - 1;
            }
        }
        occupiedSlots++;
        return pArray.get(allocChild).allocate() + allocChild * ChildEntityLockInfoArrayManager.NUM_OF_SLOTS;
    }

    void deallocate(int slotNum) {
        pArray.get(slotNum / ChildEntityLockInfoArrayManager.NUM_OF_SLOTS).deallocate(
                slotNum % ChildEntityLockInfoArrayManager.NUM_OF_SLOTS);
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
     * by calculating totalNumOfSlots = pArray.size() * ChildEntityLockInfoArrayManager.NUM_OF_SLOTS.
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

        if (size > 1 && size * ChildEntityLockInfoArrayManager.NUM_OF_SLOTS / usedSlots >= 3) {
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
     * shrink pArray according to the deinitialized children's contiguity status.
     * It doesn't deinitialze or shrink more than half of children at a time.
     */
    private void shrink() {
        int i;
        int removeCount = 0;
        int size = pArray.size();
        int maxDecreaseCount = size / 2;
        ChildEntityLockInfoArrayManager child;

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
        StringBuilder s = new StringBuilder("\n########### EntityLockInfoManager Status #############\n");
        int size = pArray.size();
        ChildEntityLockInfoArrayManager child;

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
        StringBuilder sb = new StringBuilder("\n\t########### EntityLockInfoManager Status #############\n");
        int size = pArray.size();
        ChildEntityLockInfoArrayManager child;

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

    //debugging method
    public String printWaiters(int slotNum) {
        StringBuilder s = new StringBuilder();
        int waiterObjId;
        LockWaiter waiterObj;
        int entityInfo;

        s.append("WID\tWCT\tEID\tJID\tDID\tPK\n");

        waiterObjId = getFirstWaiter(slotNum);
        while (waiterObjId != -1) {
            waiterObj = lockWaiterManager.getLockWaiter(waiterObjId);
            entityInfo = waiterObj.getEntityInfoSlot();
            s.append(waiterObjId).append("\t").append(waiterObj.getWaiterCount()).append("\t").append(entityInfo)
                    .append("\t").append(entityInfoManager.getJobId(entityInfo)).append("\t")
                    .append(entityInfoManager.getDatasetId(entityInfo)).append("\t")
                    .append(entityInfoManager.getPKHashVal(entityInfo)).append("\n");
            waiterObjId = waiterObj.getNextWaiterObjId();
        }

        return s.toString();
    }

    public void addHolder(int slotNum, int holder) {
        entityInfoManager.setPrevEntityActor(holder, getLastHolder(slotNum));
        setLastHolder(slotNum, holder);
    }

    /**
     * Remove holder from linked list of Actor.
     * Also, remove the corresponding resource from linked list of resource
     * in order to minimize JobInfo's resource link traversal.
     * 
     * @param slotNum
     * @param holder
     * @param jobInfo
     */
    public void removeHolder(int slotNum, int holder, JobInfo jobInfo) {
        int prev = getLastHolder(slotNum);
        int current = -1;
        int next;

        //remove holder from linked list of Actor
        while (prev != holder) {
            if (LockManager.IS_DEBUG_MODE) {
                if (prev == -1) {
                    //shouldn't occur: debugging purpose
                    try {
                        throw new Exception();
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }

            current = prev;
            prev = entityInfoManager.getPrevEntityActor(current);
        }

        if (current != -1) {
            //current->prev = prev->prev
            entityInfoManager.setPrevEntityActor(current, entityInfoManager.getPrevEntityActor(prev));
        } else {
            //lastHolder = prev->prev
            setLastHolder(slotNum, entityInfoManager.getPrevEntityActor(prev));
        }

        //Notice!!
        //remove the corresponding resource from linked list of resource.
        prev = entityInfoManager.getPrevJobResource(holder);
        next = entityInfoManager.getNextJobResource(holder);

        if (prev != -1) {
            entityInfoManager.setNextJobResource(prev, next);
        }

        if (next != -1) {
            entityInfoManager.setPrevJobResource(next, prev);
        } else {
            //This entityInfo(i.e., holder) is the last resource held by this job.
            jobInfo.setlastHoldingResource(prev);
        }

        //jobInfo.decreaseDatasetLockCount(holder);
    }

    public void addWaiter(int slotNum, int waiterObjId) {
        int lastObjId;
        LockWaiter lastObj = null;
        int firstWaiter = getFirstWaiter(slotNum);

        if (firstWaiter != -1) {
            //find the lastWaiter
            lastObjId = firstWaiter;
            while (lastObjId != -1) {
                lastObj = lockWaiterManager.getLockWaiter(lastObjId);
                lastObjId = lastObj.getNextWaiterObjId();
            }
            //last->next = new_waiter
            lastObj.setNextWaiterObjId(waiterObjId);
        } else {
            setFirstWaiter(slotNum, waiterObjId);
        }
        //new_waiter->next = -1
        lastObj = lockWaiterManager.getLockWaiter(waiterObjId);
        lastObj.setNextWaiterObjId(-1);
    }

    public void removeWaiter(int slotNum, int waiterObjId) {
        int currentObjId = getFirstWaiter(slotNum);
        LockWaiter currentObj;
        LockWaiter prevObj = null;
        int prevObjId = -1;
        int nextObjId;

        while (currentObjId != waiterObjId) {

            if (LockManager.IS_DEBUG_MODE) {
                if (currentObjId == -1) {
                    //shouldn't occur: debugging purpose
                    try {
                        throw new Exception();
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }

            prevObj = lockWaiterManager.getLockWaiter(currentObjId);
            prevObjId = currentObjId;
            currentObjId = prevObj.getNextWaiterObjId();
        }

        //get current waiter object
        currentObj = lockWaiterManager.getLockWaiter(currentObjId);

        //get next waiterObjId
        nextObjId = currentObj.getNextWaiterObjId();

        if (prevObjId != -1) {
            //prev->next = next
            prevObj.setNextWaiterObjId(nextObjId);
        } else {
            //removed first waiter. firstWaiter = current->next
            setFirstWaiter(slotNum, nextObjId);
        }
    }

    public void addUpgrader(int slotNum, int waiterObjId) {
        //[Notice]
        //Even if there are multiple threads in a job try to upgrade lock mode on same resource which is entity-granule,
        //while the first upgrader is waiting, all the incoming upgrade requests from other threads should be rejected by aborting them.
        //Therefore, there is no actual "ADD" upgrader method. Instead, it only has "SET" upgrader method.
        if (LockManager.IS_DEBUG_MODE) {
            if (getUpgrader(slotNum) != -1) {
                throw new IllegalStateException("Invalid lock upgrade request. This call should be handled as deadlock");
            }
        }

        setUpgrader(slotNum, waiterObjId);
    }

    public void removeUpgrader(int slotNum, int waiterObjId) {
        setUpgrader(slotNum, -1);
    }

    public boolean isUpgradeCompatible(int slotNum, byte lockMode, int entityInfo) {
        switch (lockMode) {
            case LockMode.X:
                return getSCount(slotNum) - entityInfoManager.getEntityLockCount(entityInfo) == 0;

            default:
                throw new IllegalStateException("Invalid upgrade lock mode");
        }
    }

    public boolean isCompatible(int slotNum, byte lockMode) {
        switch (lockMode) {
            case LockMode.X:
                return getSCount(slotNum) == 0 && getXCount(slotNum) == 0;

            case LockMode.S:
                return getXCount(slotNum) == 0;

            default:
                throw new IllegalStateException("Invalid upgrade lock mode");
        }
    }

    public int findEntityInfoFromHolderList(int eLockInfo, int jobId, int hashVal) {
        int entityInfo = getLastHolder(eLockInfo);

        while (entityInfo != -1) {
            if (jobId == entityInfoManager.getJobId(entityInfo)
                    && hashVal == entityInfoManager.getPKHashVal(entityInfo)) {
                return entityInfo;
            }
            //            if (LockManager.IS_DEBUG_MODE) {
            //                System.out.println("eLockInfo(" + eLockInfo + "),entityInfo(" + entityInfo + "), Request[" + jobId
            //                        + "," + hashVal + "]:Result[" + entityInfoManager.getJobId(entityInfo) + ","
            //                        + entityInfoManager.getPKHashVal(entityInfo) + "]");
            //            }
            entityInfo = entityInfoManager.getPrevEntityActor(entityInfo);
        }

        return -1;
    }

    public int findWaiterFromWaiterList(int eLockInfo, int jobId, int hashVal) {
        int waiterObjId = getFirstWaiter(eLockInfo);
        LockWaiter waiterObj;
        int entityInfo;

        while (waiterObjId != -1) {
            waiterObj = lockWaiterManager.getLockWaiter(waiterObjId);
            entityInfo = waiterObj.getEntityInfoSlot();
            if (jobId == entityInfoManager.getJobId(entityInfo)
                    && hashVal == entityInfoManager.getPKHashVal(entityInfo)) {
                return waiterObjId;
            }
            waiterObjId = waiterObj.getNextWaiterObjId();
        }

        return -1;
    }

    public int findUpgraderFromUpgraderList(int eLockInfo, int jobId, int hashVal) {
        int waiterObjId = getUpgrader(eLockInfo);
        LockWaiter waiterObj;
        int entityInfo;

        if (waiterObjId != -1) {
            waiterObj = lockWaiterManager.getLockWaiter(waiterObjId);
            entityInfo = waiterObj.getEntityInfoSlot();
            if (jobId == entityInfoManager.getJobId(entityInfo)
                    && hashVal == entityInfoManager.getPKHashVal(entityInfo)) {
                return waiterObjId;
            }
        }

        return -1;
    }

    public void increaseLockCount(int slotNum, byte lockMode) {
        switch (lockMode) {
            case LockMode.X:
                setXCount(slotNum, (short) (getXCount(slotNum) + 1));
                break;
            case LockMode.S:
                setSCount(slotNum, (short) (getSCount(slotNum) + 1));
                break;
            default:
                throw new IllegalStateException("Invalid entity lock mode " + lockMode);
        }
    }

    public void decreaseLockCount(int slotNum, byte lockMode) {
        switch (lockMode) {
            case LockMode.X:
                setXCount(slotNum, (short) (getXCount(slotNum) - 1));
                break;
            case LockMode.S:
                setSCount(slotNum, (short) (getSCount(slotNum) - 1));
                break;
            default:
                throw new IllegalStateException("Invalid entity lock mode " + lockMode);
        }
    }

    public void increaseLockCount(int slotNum, byte lockMode, short count) {
        switch (lockMode) {
            case LockMode.X:
                setXCount(slotNum, (short) (getXCount(slotNum) + count));
                break;
            case LockMode.S:
                setSCount(slotNum, (short) (getSCount(slotNum) + count));
                break;
            default:
                throw new IllegalStateException("Invalid entity lock mode " + lockMode);
        }
    }

    public void decreaseLockCount(int slotNum, byte lockMode, short count) {
        switch (lockMode) {
            case LockMode.X:
                setXCount(slotNum, (short) (getXCount(slotNum) - count));
                break;
            case LockMode.S:
                setSCount(slotNum, (short) (getSCount(slotNum) - count));
                break;
            default:
                throw new IllegalStateException("Invalid entity lock mode " + lockMode);
        }
    }

    //////////////////////////////////////////////////////////////////
    //   set/get method for each field of EntityLockInfo
    //////////////////////////////////////////////////////////////////

    public void setXCount(int slotNum, short count) {
        pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).setXCount(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS, count);
    }

    public short getXCount(int slotNum) {
        return pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).getXCount(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS);
    }

    public void setSCount(int slotNum, short count) {
        pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).setSCount(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS, count);
    }

    public short getSCount(int slotNum) {
        return pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).getSCount(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS);
    }

    public void setLastHolder(int slotNum, int holder) {
        pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).setLastHolder(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS, holder);
    }

    public int getLastHolder(int slotNum) {
        return pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).getLastHolder(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS);
    }

    public void setFirstWaiter(int slotNum, int waiter) {
        pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).setFirstWaiter(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS, waiter);
    }

    public int getFirstWaiter(int slotNum) {
        return pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).getFirstWaiter(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS);
    }

    public void setUpgrader(int slotNum, int upgrader) {
        pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).setUpgrader(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS, upgrader);
    }

    public int getUpgrader(int slotNum) {
        return pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).getUpgrader(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS);
    }

}

/******************************************
 * EntityLockInfo (16 bytes)
 * ****************************************
 * short XCount : used to represent the count of X mode lock if it is allocated. Otherwise, it represents next free slot.
 * short SCount
 * int lastHolder
 * int firstWaiter
 * int upgrader : may exist only one since there are only S and X mode lock in Entity-level
 *******************************************/

class ChildEntityLockInfoArrayManager {
    public static final int ENTITY_LOCK_INFO_SIZE = 16; //16bytes
    public static final int NUM_OF_SLOTS = 1024; //number of entityLockInfos in a buffer
    //public static final int NUM_OF_SLOTS = 10; //for unit test
    public static final int BUFFER_SIZE = ENTITY_LOCK_INFO_SIZE * NUM_OF_SLOTS;

    //byte offset of each field of EntityLockInfo
    public static final int XCOUNT_OFFSET = 0;
    public static final int SCOUNT_OFFSET = 2;
    public static final int LAST_HOLDER_OFFSET = 4;
    public static final int FIRST_WAITER_OFFSET = 8;
    public static final int UPGRADER_OFFSET = 12;

    //byte offset of nextFreeSlotNum which shares the same space with LastHolder field
    //If a slot is in use, the space is used for LastHolder. Otherwise, it is used for nextFreeSlotNum. 
    public static final int NEXT_FREE_SLOT_OFFSET = 4;

    private ByteBuffer buffer;
    private int freeSlotNum;
    private int occupiedSlots; //-1 represents 'deinitialized' state.

    public ChildEntityLockInfoArrayManager() {
        initialize();
    }

    public void initialize() {
        this.buffer = ByteBuffer.allocate(BUFFER_SIZE);
        this.freeSlotNum = 0;
        this.occupiedSlots = 0;

        for (int i = 0; i < NUM_OF_SLOTS - 1; i++) {
            setNextFreeSlot(i, i + 1);
        }
        setNextFreeSlot(NUM_OF_SLOTS - 1, -1); //-1 represents EOL(end of link)
    }

    public int allocate() {
        int currentSlot = freeSlotNum;
        freeSlotNum = getNextFreeSlot(currentSlot);
        //initialize values
        setXCount(currentSlot, (short) 0);
        setSCount(currentSlot, (short) 0);
        setLastHolder(currentSlot, -1);
        setFirstWaiter(currentSlot, -1);
        setUpgrader(currentSlot, -1);
        occupiedSlots++;
        if (LockManager.IS_DEBUG_MODE) {
            System.out.println(Thread.currentThread().getName() + " Allocated ELockInfo[" + currentSlot + "]");
        }
        return currentSlot;
    }

    public void deallocate(int slotNum) {
        setNextFreeSlot(slotNum, freeSlotNum);
        freeSlotNum = slotNum;
        occupiedSlots--;
        if (LockManager.IS_DEBUG_MODE) {
            System.out.println(Thread.currentThread().getName() + " Deallocated ELockInfo[" + slotNum + "]");
        }
    }

    public void deinitialize() {
        buffer = null;
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
        StringBuilder sb = new StringBuilder();
        sb.append("\n\toccupiedSlots:" + getNumOfOccupiedSlots());
        sb.append("\n\tfreeSlotNum:" + getFreeSlotNum());
        sb.append("\n\tX\t").append("S\t").append("LH\t").append("FW\t").append("UP\n");
        for (int j = 0; j < ChildEntityLockInfoArrayManager.NUM_OF_SLOTS; j++) {
            sb.append(j).append(": ");
            sb.append("\t" + getXCount(j));
            sb.append("\t" + getSCount(j));
            sb.append("\t" + getLastHolder(j));
            sb.append("\t" + getFirstWaiter(j));
            sb.append("\t" + getUpgrader(j));
            sb.append("\n");
        }
        return sb.toString();
    }

    //////////////////////////////////////////////////////////////////
    //   set/get method for each field of EntityLockInfo plus freeSlot
    //////////////////////////////////////////////////////////////////

    public void setNextFreeSlot(int slotNum, int nextFreeSlot) {
        buffer.putInt(slotNum * ENTITY_LOCK_INFO_SIZE + NEXT_FREE_SLOT_OFFSET, nextFreeSlot);
    }

    public int getNextFreeSlot(int slotNum) {
        return buffer.getInt(slotNum * ENTITY_LOCK_INFO_SIZE + NEXT_FREE_SLOT_OFFSET);
    }

    public void setXCount(int slotNum, short count) {
        buffer.putShort(slotNum * ENTITY_LOCK_INFO_SIZE + XCOUNT_OFFSET, count);
    }

    public short getXCount(int slotNum) {
        return buffer.getShort(slotNum * ENTITY_LOCK_INFO_SIZE + XCOUNT_OFFSET);
    }

    public void setSCount(int slotNum, short count) {
        buffer.putShort(slotNum * ENTITY_LOCK_INFO_SIZE + SCOUNT_OFFSET, count);
    }

    public short getSCount(int slotNum) {
        return buffer.getShort(slotNum * ENTITY_LOCK_INFO_SIZE + SCOUNT_OFFSET);
    }

    public void setLastHolder(int slotNum, int holder) {
        buffer.putInt(slotNum * ENTITY_LOCK_INFO_SIZE + LAST_HOLDER_OFFSET, holder);
    }

    public int getLastHolder(int slotNum) {
        return buffer.getInt(slotNum * ENTITY_LOCK_INFO_SIZE + LAST_HOLDER_OFFSET);
    }

    public void setFirstWaiter(int slotNum, int waiter) {
        buffer.putInt(slotNum * ENTITY_LOCK_INFO_SIZE + FIRST_WAITER_OFFSET, waiter);
    }

    public int getFirstWaiter(int slotNum) {
        return buffer.getInt(slotNum * ENTITY_LOCK_INFO_SIZE + FIRST_WAITER_OFFSET);
    }

    public void setUpgrader(int slotNum, int upgrader) {
        buffer.putInt(slotNum * ENTITY_LOCK_INFO_SIZE + UPGRADER_OFFSET, upgrader);
    }

    public int getUpgrader(int slotNum) {
        return buffer.getInt(slotNum * ENTITY_LOCK_INFO_SIZE + UPGRADER_OFFSET);
    }
}