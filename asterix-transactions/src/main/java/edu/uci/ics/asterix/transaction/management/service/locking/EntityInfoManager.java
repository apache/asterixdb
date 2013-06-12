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

/**
 * EntityInfoManager provides EntityInfo arrays backed by ByteBuffer.
 * The array grows when the slots are overflowed.
 * Also, the array shrinks according to the following shrink policy
 * : Shrink when the resource under-utilization lasts for a certain threshold time.
 * 
 * @author kisskys
 */
public class EntityInfoManager {

    private ArrayList<ChildEntityInfoArrayManager> pArray;
    private int allocChild; //used to allocate the next free EntityInfo slot.
    private long shrinkTimer;
    private boolean isShrinkTimerOn;
    private int occupiedSlots;
    private int shrinkTimerThreshold;

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
    //        int slots = ChildEntityInfoArrayManager.NUM_OF_SLOTS;
    //        int data[] = new int[DataSize];
    //        EntityInfoManager eiMgr = new EntityInfoManager();
    //
    //        //allocate: 50
    //        System.out.println("allocate: 50");
    //        for (i = 0; i < 5; i++) {
    //            for (j = i * slots; j < i * slots + slots; j++) {
    //                data[j] = eiMgr.allocate();
    //            }
    //
    //            System.out.println(eiMgr.prettyPrint());
    //        }
    //
    //        //deallocate from the last child to the first child
    //        System.out.println("deallocate from the last child to the first child");
    //        for (i = 4; i >= 0; i--) {
    //            for (j = i * slots + slots - 1; j >= i * slots; j--) {
    //                eiMgr.deallocate(data[j]);
    //            }
    //            System.out.println(eiMgr.prettyPrint());
    //        }
    //
    //        //allocate: 50
    //        System.out.println("allocate: 50");
    //        for (i = 0; i < 5; i++) {
    //            for (j = i * slots; j < i * slots + slots; j++) {
    //                data[j] = eiMgr.allocate();
    //            }
    //
    //            System.out.println(eiMgr.prettyPrint());
    //        }
    //
    //        //deallocate from the first child to last child
    //        System.out.println("deallocate from the first child to last child");
    //        for (i = 0; i < 5; i++) {
    //            for (j = i * slots; j < i * slots + slots; j++) {
    //                eiMgr.deallocate(data[j]);
    //            }
    //
    //            System.out.println(eiMgr.prettyPrint());
    //        }
    //
    //        //allocate: 50
    //        System.out.println("allocate: 50");
    //        for (i = 0; i < 5; i++) {
    //            for (j = i * slots; j < i * slots + slots; j++) {
    //                data[j] = eiMgr.allocate();
    //            }
    //
    //            System.out.println(eiMgr.prettyPrint());
    //        }
    //
    //        //deallocate from the first child to 4th child
    //        System.out.println("deallocate from the first child to 4th child");
    //        for (i = 0; i < 4; i++) {
    //            for (j = i * slots; j < i * slots + slots; j++) {
    //                eiMgr.deallocate(data[j]);
    //            }
    //
    //            System.out.println(eiMgr.prettyPrint());
    //        }
    //
    //        //allocate: 40
    //        System.out.println("allocate: 40");
    //        for (i = 0; i < 4; i++) {
    //            for (j = i * slots; j < i * slots + slots; j++) {
    //                data[j] = eiMgr.allocate();
    //            }
    //
    //            System.out.println(eiMgr.prettyPrint());
    //        }
    //    }
    //    
    //    ////////////////////////////////////////////////
    //    // end of unit test
    //    ////////////////////////////////////////////////

    public EntityInfoManager(int shrinkTimerThreshold) {
        pArray = new ArrayList<ChildEntityInfoArrayManager>();
        pArray.add(new ChildEntityInfoArrayManager());
        allocChild = 0;
        occupiedSlots = 0;
        isShrinkTimerOn = false;
        this.shrinkTimerThreshold = shrinkTimerThreshold;
    }

    public int allocate(int jobId, int datasetId, int entityHashVal, byte lockMode) {
        int slotNum = allocate();
        initEntityInfo(slotNum, jobId, datasetId, entityHashVal, lockMode);
        return slotNum;
    }

    public int allocate() {
        if (pArray.get(allocChild).isFull()) {
            int size = pArray.size();
            boolean bAlloc = false;
            ChildEntityInfoArrayManager child;

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
                pArray.add(new ChildEntityInfoArrayManager());
                allocChild = pArray.size() - 1;
            }
        }

        occupiedSlots++;
        return pArray.get(allocChild).allocate() + allocChild * ChildEntityInfoArrayManager.NUM_OF_SLOTS;
    }

    void deallocate(int slotNum) {
        pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).deallocate(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS);
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
     * by calculating totalNumOfSlots = pArray.size() * ChildEntityInfoArrayManager.NUM_OF_SLOTS.
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

        if (size > 1 && size * ChildEntityInfoArrayManager.NUM_OF_SLOTS / usedSlots >= 3) {
            if (isShrinkTimerOn) {
                if (System.currentTimeMillis() - shrinkTimer >= shrinkTimerThreshold) {
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
        ChildEntityInfoArrayManager child;

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
        StringBuilder s = new StringBuilder("\n########### EntityInfoManager Status #############\n");
        int size = pArray.size();
        ChildEntityInfoArrayManager child;

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
        ChildEntityInfoArrayManager child;

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
        return shrinkTimerThreshold;
    }

    public void initEntityInfo(int slotNum, int jobId, int datasetId, int PKHashVal, byte lockMode) {
        pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).initEntityInfo(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS, jobId, datasetId, PKHashVal, lockMode);
    }

    public boolean compareEntityInfo(int slotNum, int jobId, int datasetId, int PKHashVal) {
        return getPKHashVal(slotNum) == PKHashVal && getDatasetId(slotNum) == datasetId && getJobId(slotNum) == jobId;
    }

    public void increaseDatasetLockCount(int slotNum) {
        setDatasetLockCount(slotNum, (byte) (getDatasetLockCount(slotNum) + 1));
    }

    public void decreaseDatasetLockCount(int slotNum) {
        setDatasetLockCount(slotNum, (byte) (getDatasetLockCount(slotNum) - 1));
    }

    public void increaseEntityLockCount(int slotNum) {
        setEntityLockCount(slotNum, (byte) (getEntityLockCount(slotNum) + 1));
    }

    public void decreaseEntityLockCount(int slotNum) {
        setEntityLockCount(slotNum, (byte) (getEntityLockCount(slotNum) - 1));
    }

    public void increaseDatasetLockCount(int slotNum, int count) {
        setDatasetLockCount(slotNum, (byte) (getDatasetLockCount(slotNum) + count));
    }

    public void decreaseDatasetLockCount(int slotNum, int count) {
        setDatasetLockCount(slotNum, (byte) (getDatasetLockCount(slotNum) - count));
    }

    public void increaseEntityLockCount(int slotNum, int count) {
        setEntityLockCount(slotNum, (byte) (getEntityLockCount(slotNum) + count));
    }

    public void decreaseEntityLockCount(int slotNum, int count) {
        setEntityLockCount(slotNum, (byte) (getEntityLockCount(slotNum) - count));
    }

    //////////////////////////////////////////////////////////////////
    //   set/get method for each field of EntityInfo
    //////////////////////////////////////////////////////////////////

    public void setJobId(int slotNum, int id) {
        pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).setJobId(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS, id);
    }

    public int getJobId(int slotNum) {
        return pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).getJobId(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS);
    }

    public void setDatasetId(int slotNum, int id) {
        pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).setDatasetId(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS, id);
    }

    public int getDatasetId(int slotNum) {
        return pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).getDatasetId(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS);
    }

    public void setPKHashVal(int slotNum, int hashVal) {
        pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).setPKHashVal(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS, hashVal);
    }

    public int getPKHashVal(int slotNum) {
        return pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).getPKHashVal(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS);
    }

    public void setDatasetLockMode(int slotNum, byte mode) {
        pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).setDatasetLockMode(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS, mode);
    }

    public byte getDatasetLockMode(int slotNum) {
        return pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).getDatasetLockMode(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS);
    }

    public void setDatasetLockCount(int slotNum, byte count) {
        pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).setDatasetLockCount(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS, count);
    }

    public byte getDatasetLockCount(int slotNum) {
        return pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).getDatasetLockCount(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS);
    }

    public void setEntityLockMode(int slotNum, byte mode) {
        pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).setEntityLockMode(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS, mode);
    }

    public byte getEntityLockMode(int slotNum) {
        return pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).getEntityLockMode(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS);
    }

    public void setEntityLockCount(int slotNum, byte count) {
        pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).setEntityLockCount(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS, count);
    }

    public byte getEntityLockCount(int slotNum) {
        return pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).getEntityLockCount(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS);
    }

    //Used for Waiter/Upgrader
    public void setNextEntityActor(int slotNum, int nextActorSlotNum) {
        pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).setNextEntityActor(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS, nextActorSlotNum);
    }

    //Used for Waiter/Upgrader
    public int getNextEntityActor(int slotNum) {
        return pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).getNextEntityActor(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS);
    }

    //Used for Holder
    public void setPrevEntityActor(int slotNum, int nextActorSlotNum) {
        pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).setPrevEntityActor(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS, nextActorSlotNum);
    }

    //Used for Holder
    public int getPrevEntityActor(int slotNum) {
        return pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).getPrevEntityActor(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS);
    }

    public void setPrevJobResource(int slotNum, int prevResourceSlotNum) {
        pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).setPrevJobResource(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS, prevResourceSlotNum);
    }

    public int getPrevJobResource(int slotNum) {
        return pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).getPrevJobResource(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS);
    }

    public void setNextJobResource(int slotNum, int nextResourceSlotNum) {
        pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).setNextJobResource(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS, nextResourceSlotNum);
    }

    public int getNextJobResource(int slotNum) {
        return pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).getNextJobResource(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS);
    }

    //    public void setNextDatasetActor(int slotNum, int nextActorSlotNum) {
    //        pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).setNextDatasetActor(
    //                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS, nextActorSlotNum);
    //    }
    //
    //    public int getNextDatasetActor(int slotNum) {
    //        return pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).getNextDatasetActor(
    //                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS);
    //    }
}

/******************************************
 * EntityInfo (28 bytes)
 * ****************************************
 * int jobId
 * int datasetId
 * int PKHashValue
 * byte datasetLockMode
 * byte datasetLockCount
 * byte enitityLockMode
 * byte entityLockCount
 * int nextEntityActor : actor can be either holder/waiter/upgrader
 * int prevJobResource : resource can be either dataset or entity and a job is holding/waiting/upgrading lock(s) on it.
 * int nextJobResource : resource can be either dataset or entity and a job is holding/waiting/upgrading lock(s) on it.
 * (int nextDatasetActor : actor can be either holder/waiter/upgrader) --> not used.
 *******************************************/

class ChildEntityInfoArrayManager {
    public static final int ENTITY_INFO_SIZE = 28; //28bytes
    public static final int NUM_OF_SLOTS = 1024; //number of entities in a buffer
    //    public static final int NUM_OF_SLOTS = 10; //for unit test
    public static final int BUFFER_SIZE = ENTITY_INFO_SIZE * NUM_OF_SLOTS;

    //byte offset of each field of EntityInfo
    public static final int JOB_ID_OFFSET = 0;
    public static final int DATASET_ID_OFFSET = 4;
    public static final int PKHASH_VAL_OFFSET = 8;
    public static final int DATASET_LOCK_MODE_OFFSET = 12;
    public static final int DATASET_LOCK_COUNT_OFFSET = 13;
    public static final int ENTITY_LOCK_MODE_OFFSET = 14;
    public static final int ENTITY_LOCK_COUNT_OFFSET = 15;
    public static final int ENTITY_ACTOR_OFFSET = 16;
    public static final int PREV_JOB_RESOURCE_OFFSET = 20;
    public static final int NEXT_JOB_RESOURCE_OFFSET = 24;
    //public static final int DATASET_ACTOR_OFFSET = 28;

    //byte offset of nextFreeSlotNum which shares the same space of JobId
    //If a slot is in use, the space is used for JobId. Otherwise, it is used for nextFreeSlotNum. 
    public static final int NEXT_FREE_SLOT_OFFSET = 0;

    private ByteBuffer buffer;
    private int freeSlotNum;
    private int occupiedSlots; //-1 represents 'deinitialized' state.

    public ChildEntityInfoArrayManager() {
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
        occupiedSlots++;
        if (LockManager.IS_DEBUG_MODE) {
            System.out.println(Thread.currentThread().getName() + " entity allocate: " + currentSlot);
        }
        return currentSlot;
    }

    public void deallocate(int slotNum) {
        setNextFreeSlot(slotNum, freeSlotNum);
        freeSlotNum = slotNum;
        occupiedSlots--;
        if (LockManager.IS_DEBUG_MODE) {
            System.out.println(Thread.currentThread().getName() + " entity deallocate: " + slotNum);
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
        sb.append("\n\tfreeSlotNum:" + getFreeSlotNum() + "\n");
        sb.append("\tjid\t").append("did\t").append("PK\t").append("DLM\t").append("DLC\t").append("ELM\t")
                .append("ELC\t").append("NEA\t").append("PJR\t").append("NJR\n");
        for (int j = 0; j < ChildEntityInfoArrayManager.NUM_OF_SLOTS; j++) {
            sb.append(j).append(": ");
            sb.append("\t" + getJobId(j));
            sb.append("\t" + getDatasetId(j));
            sb.append("\t" + getPKHashVal(j));
            sb.append("\t" + getDatasetLockMode(j));
            sb.append("\t" + getDatasetLockCount(j));
            sb.append("\t" + getEntityLockMode(j));
            sb.append("\t" + getEntityLockCount(j));
            sb.append("\t" + getNextEntityActor(j));
            sb.append("\t" + getPrevJobResource(j));
            sb.append("\t" + getNextJobResource(j));
            sb.append("\n");
        }
        return sb.toString();
    }

    //////////////////////////////////////////////////////////////////
    //   set/get method for each field of EntityInfo plus freeSlot
    //////////////////////////////////////////////////////////////////
    public void initEntityInfo(int slotNum, int jobId, int datasetId, int PKHashVal, byte lockMode) {
        buffer.putInt(slotNum * ENTITY_INFO_SIZE + JOB_ID_OFFSET, jobId);
        buffer.putInt(slotNum * ENTITY_INFO_SIZE + DATASET_ID_OFFSET, datasetId);
        buffer.putInt(slotNum * ENTITY_INFO_SIZE + PKHASH_VAL_OFFSET, PKHashVal);
        buffer.put(slotNum * ENTITY_INFO_SIZE + DATASET_LOCK_MODE_OFFSET, lockMode);
        buffer.put(slotNum * ENTITY_INFO_SIZE + DATASET_LOCK_COUNT_OFFSET, (byte) 0);
        buffer.put(slotNum * ENTITY_INFO_SIZE + ENTITY_LOCK_MODE_OFFSET, lockMode);
        buffer.put(slotNum * ENTITY_INFO_SIZE + ENTITY_LOCK_COUNT_OFFSET, (byte) 0);
        buffer.putInt(slotNum * ENTITY_INFO_SIZE + ENTITY_ACTOR_OFFSET, -1);
        buffer.putInt(slotNum * ENTITY_INFO_SIZE + PREV_JOB_RESOURCE_OFFSET, -1);
        buffer.putInt(slotNum * ENTITY_INFO_SIZE + NEXT_JOB_RESOURCE_OFFSET, -1);
        //buffer.putInt(slotNum * ENTITY_INFO_SIZE + DATASET_ACTOR_OFFSET, -1);
    }

    public void setNextFreeSlot(int slotNum, int nextFreeSlot) {
        buffer.putInt(slotNum * ENTITY_INFO_SIZE + NEXT_FREE_SLOT_OFFSET, nextFreeSlot);
    }

    public int getNextFreeSlot(int slotNum) {
        return buffer.getInt(slotNum * ENTITY_INFO_SIZE + NEXT_FREE_SLOT_OFFSET);
    }

    public void setJobId(int slotNum, int id) {
        buffer.putInt(slotNum * ENTITY_INFO_SIZE + JOB_ID_OFFSET, id);
    }

    public int getJobId(int slotNum) {
        return buffer.getInt(slotNum * ENTITY_INFO_SIZE + JOB_ID_OFFSET);
    }

    public void setDatasetId(int slotNum, int id) {
        buffer.putInt(slotNum * ENTITY_INFO_SIZE + DATASET_ID_OFFSET, id);
    }

    public int getDatasetId(int slotNum) {
        return buffer.getInt(slotNum * ENTITY_INFO_SIZE + DATASET_ID_OFFSET);
    }

    public void setPKHashVal(int slotNum, int hashVal) {
        buffer.putInt(slotNum * ENTITY_INFO_SIZE + PKHASH_VAL_OFFSET, hashVal);
    }

    public int getPKHashVal(int slotNum) {
        return buffer.getInt(slotNum * ENTITY_INFO_SIZE + PKHASH_VAL_OFFSET);
    }

    public void setDatasetLockMode(int slotNum, byte mode) {
        buffer.put(slotNum * ENTITY_INFO_SIZE + DATASET_LOCK_MODE_OFFSET, mode);
    }

    public byte getDatasetLockMode(int slotNum) {
        return buffer.get(slotNum * ENTITY_INFO_SIZE + DATASET_LOCK_MODE_OFFSET);
    }

    public void setDatasetLockCount(int slotNum, byte count) {
        buffer.put(slotNum * ENTITY_INFO_SIZE + DATASET_LOCK_COUNT_OFFSET, count);
    }

    public byte getDatasetLockCount(int slotNum) {
        return buffer.get(slotNum * ENTITY_INFO_SIZE + DATASET_LOCK_COUNT_OFFSET);
    }

    public void setEntityLockMode(int slotNum, byte mode) {
        buffer.put(slotNum * ENTITY_INFO_SIZE + ENTITY_LOCK_MODE_OFFSET, mode);
    }

    public byte getEntityLockMode(int slotNum) {
        return buffer.get(slotNum * ENTITY_INFO_SIZE + ENTITY_LOCK_MODE_OFFSET);
    }

    public void setEntityLockCount(int slotNum, byte count) {
        buffer.put(slotNum * ENTITY_INFO_SIZE + ENTITY_LOCK_COUNT_OFFSET, count);
    }

    public byte getEntityLockCount(int slotNum) {
        return buffer.get(slotNum * ENTITY_INFO_SIZE + ENTITY_LOCK_COUNT_OFFSET);
    }

    //Used for Waiter/Upgrader
    public void setNextEntityActor(int slotNum, int nextActorSlotNum) {
        buffer.putInt(slotNum * ENTITY_INFO_SIZE + ENTITY_ACTOR_OFFSET, nextActorSlotNum);
    }

    //Used for Waiter/Upgrader
    public int getNextEntityActor(int slotNum) {
        return buffer.getInt(slotNum * ENTITY_INFO_SIZE + ENTITY_ACTOR_OFFSET);
    }

    //Used for Holder
    public void setPrevEntityActor(int slotNum, int nextActorSlotNum) {
        buffer.putInt(slotNum * ENTITY_INFO_SIZE + ENTITY_ACTOR_OFFSET, nextActorSlotNum);
    }

    //Used for Holder
    public int getPrevEntityActor(int slotNum) {
        return buffer.getInt(slotNum * ENTITY_INFO_SIZE + ENTITY_ACTOR_OFFSET);
    }

    public void setPrevJobResource(int slotNum, int prevResourceSlotNum) {
        buffer.putInt(slotNum * ENTITY_INFO_SIZE + PREV_JOB_RESOURCE_OFFSET, prevResourceSlotNum);
    }

    public int getPrevJobResource(int slotNum) {
        return buffer.getInt(slotNum * ENTITY_INFO_SIZE + PREV_JOB_RESOURCE_OFFSET);
    }

    public void setNextJobResource(int slotNum, int prevResourceSlotNum) {
        buffer.putInt(slotNum * ENTITY_INFO_SIZE + NEXT_JOB_RESOURCE_OFFSET, prevResourceSlotNum);
    }

    public int getNextJobResource(int slotNum) {
        return buffer.getInt(slotNum * ENTITY_INFO_SIZE + NEXT_JOB_RESOURCE_OFFSET);
    }

    //    public void setNextDatasetActor(int slotNum, int nextActorSlotNum) {
    //        buffer.putInt(slotNum * ENTITY_INFO_SIZE + DATASET_ACTOR_OFFSET, nextActorSlotNum);
    //    }
    //
    //    public int getNextDatasetActor(int slotNum) {
    //        return buffer.getInt(slotNum * ENTITY_INFO_SIZE + DATASET_ACTOR_OFFSET);
    //    }
}