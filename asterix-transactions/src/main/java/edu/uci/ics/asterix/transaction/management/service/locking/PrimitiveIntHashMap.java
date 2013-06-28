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

import java.util.ArrayList;

/**
 * PrimitiveIntHashMap supports primitive int type as key and value.
 * The hash map grows when the available slots in a bucket are overflowed.
 * Also, the hash map shrinks according to the following shrink policy.
 * : Shrink when the resource under-utilization lasts for a certain threshold time. 
 *   
 * @author kisskys
 *
 */
public class PrimitiveIntHashMap {
    private final int CHILD_BUCKETS; //INIT_NUM_OF_BUCKETS;
    private final int NUM_OF_SLOTS; //NUM_OF_SLOTS_IN_A_BUCKET;
    private final int SHRINK_TIMER_THRESHOLD;
    
    private int occupiedSlots;
    private ArrayList<ChildIntArrayManager> pArray; //parent array
    private int hashMod;
    private long shrinkTimer;
    private boolean isShrinkTimerOn;
    private int iterBucketIndex;
    private int iterSlotIndex;
    private int iterChildIndex;
    private KeyValuePair iterPair;

//    ////////////////////////////////////////////////
//    // begin of unit test
//    ////////////////////////////////////////////////
//
//    /**
//     * @param args
//     */
//    public static void main(String[] args) {
//        int i, j;
//        int k = 0;
//        int num = 5;
//        int key[] = new int[500];
//        int val[] = new int[500];
//        KeyValuePair pair;
//        PrimitiveIntHashMap map = new PrimitiveIntHashMap(1<<4, 1<<3, 5);
//        
//        for (j=0; j < num; j++) {
//            
//            k += 100;
//            //generate data
//            for (i=0; i < k; i++) {
//                key[i] = i;
//                val[i] = i;
//            }
//            
//            //put data to map
//            for (i=0; i < k-30; i++) {
//                map.put(key[i], val[i]);
//            }
//            
//            //put data to map
//            for (i=0; i < k-30; i++) {
//                map.put(key[i], val[i]);
//            }
//            
//            map.beginIterate();
//            pair = map.getNextKeyValue();
//            i = 0;
//            while (pair != null) {
//                i++;
//                System.out.println("["+i+"] key:"+ pair.key + ", val:"+ pair.value);
//                pair = map.getNextKeyValue();
//            }
//            
//            //System.out.println(map.prettyPrint());
//            
//            for (i=k-20; i< k; i++) { //skip X70~X79
//                map.put(key[i], val[i]);
//            }
//            
//            System.out.println(map.prettyPrint());
//            
//            //remove data to map
//            for (i=0; i < k-10; i++) { 
//                map.remove(key[i]);
//                try {
//                    Thread.currentThread().sleep(1);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
//            
//            map.beginIterate();
//            pair = map.getNextKeyValue();
//            i = 0;
//            while (pair != null) {
//                i++;
//                System.out.println("["+i+"] key:"+ pair.key + ", val:"+ pair.value);
//                pair = map.getNextKeyValue();
//            }
//            
//            //remove data to map
//            for (i=0; i < k-10; i++) { 
//                map.remove(key[i]);
//                try {
//                    Thread.currentThread().sleep(1);
//                } catch (InterruptedException e) {
//                    // TODO Auto-generated catch block
//                    e.printStackTrace();
//                }
//            }
//            
//            System.out.println(map.prettyPrint());
//            
//            //get data from map
//            for (i=0; i < k; i++) {
//                System.out.println(""+i+"=> key:"+ key[i] + ", val:"+val[i] +", result: " + map.get(key[i]));  
//            }
//        }
//        
//        map.beginIterate();
//        pair = map.getNextKeyValue();
//        i = 0;
//        while (pair != null) {
//            i++;
//            System.out.println("["+i+"] key:"+ pair.key + ", val:"+ pair.value);
//            pair = map.getNextKeyValue();
//        }
//    }
//
//    ////////////////////////////////////////////////
//    // end of unit test
//    ////////////////////////////////////////////////
    
    public PrimitiveIntHashMap() {
        CHILD_BUCKETS = 1<<9; //INIT_NUM_OF_BUCKETS;
        NUM_OF_SLOTS = 1<<3; //NUM_OF_SLOTS_IN_A_BUCKET;
        SHRINK_TIMER_THRESHOLD = 120000; //2min
        pArray = new ArrayList<ChildIntArrayManager>();
        pArray.add(new ChildIntArrayManager(this));
        hashMod = CHILD_BUCKETS;
        occupiedSlots = 0;
        iterPair = new KeyValuePair();
    }
    
    public PrimitiveIntHashMap(int childBuckets, int numOfSlots, int shrinkTimerThreshold) {
        CHILD_BUCKETS = childBuckets;
        NUM_OF_SLOTS = numOfSlots;
        SHRINK_TIMER_THRESHOLD = shrinkTimerThreshold;
        pArray = new ArrayList<ChildIntArrayManager>();
        pArray.add(new ChildIntArrayManager(this));
        hashMod = CHILD_BUCKETS;
        occupiedSlots = 0;
        iterPair = new KeyValuePair();
    }
    
    public void put(int key, int value) {
        int growCount = 0;
        int bucketNum = hash(key);
        ChildIntArrayManager child = pArray.get(bucketNum/CHILD_BUCKETS);
        while (child.isFull(bucketNum%CHILD_BUCKETS)) {
            growHashMap();
            bucketNum = hash(key);
            child = pArray.get(bucketNum/CHILD_BUCKETS);
            if (growCount > 2) {
                //changeHashFunc();
            }
            growCount++;
        }
        occupiedSlots += child.put(bucketNum%CHILD_BUCKETS, key, value, false);
    }
    
    public void upsert (int key, int value) {
        int growCount = 0;
        int bucketNum = hash(key);
        ChildIntArrayManager child = pArray.get(bucketNum/CHILD_BUCKETS);
        while (child.isFull(bucketNum%CHILD_BUCKETS)) {
            growHashMap();
            bucketNum = hash(key);
            child = pArray.get(bucketNum/CHILD_BUCKETS);
            if (growCount > 2) {
                //changeHashFunc();
            }
            growCount++;
        }
        occupiedSlots += child.put(bucketNum%CHILD_BUCKETS, key, value, true);
    }
    
    private int hash(int key) {
        return key%hashMod;
    }
    
    private void growHashMap() {
        int size = pArray.size();
        int i; 
        
        //grow buckets by adding more child
        for (i=0; i<size; i++) { 
            pArray.add(new ChildIntArrayManager(this));
        }
        
        //increase hashMod
        hashMod *= 2;
        
        //re-hash
        rehash(0, size, hashMod/2);
    }
    
    private void shrinkHashMap() {
        int size = pArray.size();
        int i;
        
        //decrease hashMod
        hashMod /= 2;
        
        //re-hash
        rehash(size/2, size, hashMod*2);
        
        //shrink buckets by removing child(s)
        for (i=size-1; i>=size/2;i--) {
            pArray.remove(i);
        }
    }
    
    private void rehash(int begin, int end, int oldHashMod) {
        int i, j, k;
        int key, value;
        ChildIntArrayManager child;
        
        //re-hash
        for (i=begin; i<end; i++) {
            child = pArray.get(i);
            for (j=0; j<CHILD_BUCKETS; j++) {
                if (child.cArray[j][0] == 0) {
                    continue;
                }
                for (k=1; k<NUM_OF_SLOTS; k++) {
                    //if the hashValue of the key is different, then re-hash it.
                    key = child.cArray[j][k*2];
                    if (hash(key) != key%oldHashMod) {
                        value = child.cArray[j][k*2+1];
                        //remove existing key and value
                        //Notice! To avoid bucket iteration, child.remove() is not used.
                        child.cArray[j][k*2] = -1;
                        child.cArray[j][0]--;
                        //re-hash it 
                        pArray.get(hash(key)/CHILD_BUCKETS).put(hash(key)%CHILD_BUCKETS, key, value, false);
                    }
                }
            }
        }
    }
    
//    private void changeHashFunc() {
//        //TODO need to implement.
//        throw new UnsupportedOperationException("changeHashFunc() not implemented");
//    }
    
    public int get(int key) {
        int bucketNum = hash(key);
        return pArray.get(bucketNum/CHILD_BUCKETS).get(bucketNum%CHILD_BUCKETS, key);
    }
    
    public void remove(int key) {
        int bucketNum = hash(key);
        occupiedSlots -= pArray.get(bucketNum/CHILD_BUCKETS).remove(bucketNum%CHILD_BUCKETS, key);
        
        if (needShrink()) {
            shrinkHashMap();
        }
    }
    
    /**
     * Shrink policy:
     * Shrink when the resource under-utilization lasts for a certain amount of time. 
     * @return
     */
    private boolean needShrink() {
        int size = pArray.size();
        int usedSlots = occupiedSlots;
        if (usedSlots == 0) {
            usedSlots = 1;
        }
        if (size > 1 && size*CHILD_BUCKETS*NUM_OF_SLOTS/usedSlots >= 3 && isSafeToShrink()) {
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
    
    private boolean isSafeToShrink() {
        int i, j;
        int size = pArray.size();
        //Child: 0, 1, 2, 3, 4, 5, 6, 7 
        //[HChild(Head Child):0 and TChild(Tail Child): 4], [1(H),5(T)], [2(H),6(T)] and so on. 
        //When the map shrinks, the sum of occupied slots in H/TChild should not exceed the NUM_OF_SLOTS-1.
        //Then it is safe to shrink. Otherwise, unsafe.
        ChildIntArrayManager HChild, TChild; 
        
        for (i=0; i<size/2; i++){
            HChild = pArray.get(i);
            TChild = pArray.get(size/2+i);
            for (j=0; j<CHILD_BUCKETS; j++) {
                if (HChild.cArray[j][0] + TChild.cArray[j][0] > NUM_OF_SLOTS-1) {
                    return false;
                }
            }
        }
        return true;
    }
    
    public String prettyPrint() {
        StringBuilder s = new StringBuilder("\n########### PrimitiveIntHashMap Status #############\n");
        ChildIntArrayManager child;
        int i, j, k;
        int size = pArray.size();
        for (i=0; i<size;i++) {
            child = pArray.get(i);
            s.append("child[").append(i).append("]\n");
            for (j=0; j<CHILD_BUCKETS;j++) {
                s.append(j).append(" ");
                for (k=0; k<NUM_OF_SLOTS;k++) {
                    s.append("[").append(child.cArray[j][k*2]).append(",").append(child.cArray[j][k*2+1]).append("] ");
                }
                s.append("\n");
            }
        }
        return s.toString();
    }
    
    public int getNumOfSlots() {
        return NUM_OF_SLOTS;
    }
    
    public int getNumOfChildBuckets() {
        return CHILD_BUCKETS;
    }
    
    public void clear(boolean needShrink) {
        int size = pArray.size();
        for (int i=size-1; i >= 0; i--) {
            if (needShrink && i != 0) {
                pArray.remove(i);
            } else {
                pArray.get(i).clear();
            }
        }
        occupiedSlots = 0;
    }
    
    ///////////////////////////////////////
    // iterate method
    ///////////////////////////////////////
    
    public void beginIterate() {
        iterChildIndex = 0;
        iterBucketIndex = 0;
        iterSlotIndex = 1;
    }
    
    public KeyValuePair getNextKeyValue() {
        for (; iterChildIndex < pArray.size(); iterChildIndex++, iterBucketIndex = 0) {
            for (; iterBucketIndex < CHILD_BUCKETS; iterBucketIndex++, iterSlotIndex = 1) {
                if (iterSlotIndex ==1 && pArray.get(iterChildIndex).cArray[iterBucketIndex][0] == 0) {
                    continue;
                }
                for (; iterSlotIndex < NUM_OF_SLOTS; iterSlotIndex++) {
                    iterPair.key = pArray.get(iterChildIndex).cArray[iterBucketIndex][iterSlotIndex*2];
                    if (iterPair.key == -1) {
                        continue;
                    }
                    iterPair.value = pArray.get(iterChildIndex).cArray[iterBucketIndex][iterSlotIndex*2+1];
                    iterSlotIndex++;
                    return iterPair;
                }
            }
        }
        return null;
    }
    
    public int getNextKey() {
        for (; iterChildIndex < pArray.size(); iterChildIndex++, iterBucketIndex = 0) {
            for (; iterBucketIndex < CHILD_BUCKETS; iterBucketIndex++, iterSlotIndex = 1) {
                if (iterSlotIndex ==1 && pArray.get(iterChildIndex).cArray[iterBucketIndex][0] == 0) {
                    continue;
                }
                for (; iterSlotIndex < NUM_OF_SLOTS; iterSlotIndex++) {
                    iterPair.key = pArray.get(iterChildIndex).cArray[iterBucketIndex][iterSlotIndex*2];
                    if (iterPair.key == -1) {
                        continue;
                    }
                    iterSlotIndex++;
                    return iterPair.key;
                }
            }
        }
        return -1;
    }
    
    public int getNextValue() {
        for (; iterChildIndex < pArray.size(); iterChildIndex++, iterBucketIndex = 0) {
            for (; iterBucketIndex < CHILD_BUCKETS; iterBucketIndex++, iterSlotIndex = 1) {
                if (iterSlotIndex ==1 && pArray.get(iterChildIndex).cArray[iterBucketIndex][0] == 0) {
                    continue;
                }
                for (; iterSlotIndex < NUM_OF_SLOTS; iterSlotIndex++) {
                    iterPair.key = pArray.get(iterChildIndex).cArray[iterBucketIndex][iterSlotIndex*2];
                    if (iterPair.key == -1) {
                        continue;
                    }
                    iterPair.value = pArray.get(iterChildIndex).cArray[iterBucketIndex][iterSlotIndex*2+1];
                    iterSlotIndex++;
                    return iterPair.value;
                }
            }
        }
        return -1;
    }
    
    public static class KeyValuePair {
        public int key;
        public int value; 
    }
}

class ChildIntArrayManager {
    private final int DIM1_SIZE; 
    private final int DIM2_SIZE; 
    private final int NUM_OF_SLOTS;
    public int[][] cArray; //child array
    
    public ChildIntArrayManager(PrimitiveIntHashMap parentHashMap) {
        DIM1_SIZE = parentHashMap.getNumOfChildBuckets();
        DIM2_SIZE = parentHashMap.getNumOfSlots() * 2; //2: Array of [key, value] pair
        NUM_OF_SLOTS = parentHashMap.getNumOfSlots() ;
        initialize();
    }

    private void initialize() {
        cArray = new int[DIM1_SIZE][DIM2_SIZE];
        int i, j;
        for (i = 0; i < DIM1_SIZE; i++) {
            //cArray[i][0] is used as a counter to count how many slots are used in this bucket.
            //cArray[i][1] is not used.
            cArray[i][0] = 0;
            for (j = 1; j < NUM_OF_SLOTS; j++) {
                cArray[i][j*2] = -1; // -1 represent that the slot is empty
            }
        }
    }
    
    public void clear() {
        int i, j;
        for (i = 0; i < DIM1_SIZE; i++) {
            //cArray[i][0] is used as a counter to count how many slots are used in this bucket.
            //cArray[i][1] is not used.
            if (cArray[i][0] == 0) {
                continue;
            }
            cArray[i][0] = 0;
            for (j = 1; j < NUM_OF_SLOTS; j++) {
                cArray[i][j*2] = -1; // -1 represent that the slot is empty
            }
        }
    }
    
    public void deinitialize() {
        cArray = null;
    }
    
    public void allocate() {
        initialize();
    }

    public boolean isFull(int bucketNum) {
        return cArray[bucketNum][0] == NUM_OF_SLOTS-1;
    }
    
    public boolean isEmpty(int bucketNum) {
        return cArray[bucketNum][0] == 0;
    }

    /**
     * Put key,value into a slot in the bucket if the key doesn't exist.
     * Update value if the key exists and if isUpsert is true
     * No need to call get() to check the existence of the key before calling put().
     * Notice! Caller should make sure that there is an available slot.
     * 
     * @param bucketNum
     * @param key
     * @param value
     * @param isUpsert
     * @return 1 for new insertion, 0 for key duplication 
     */
    public int put(int bucketNum, int key, int value, boolean isUpsert) {
        int i;
        int emptySlot=-1;

        if (cArray[bucketNum][0] == 0) {
            cArray[bucketNum][2] = key;
            cArray[bucketNum][3] = value;
            cArray[bucketNum][0]++;
            return 1;
        }

        for (i = 1; i < NUM_OF_SLOTS; i++) {
            if (cArray[bucketNum][i*2] == key) {
                if (isUpsert) {
                    cArray[bucketNum][i*2+1] = value;
                }
                return 0;
            }
            else if (cArray[bucketNum][i*2] == -1) {
                emptySlot = i;
            }
        }
        
        if (emptySlot == -1) {
            throw new UnsupportedOperationException("error");
        }
        
        cArray[bucketNum][emptySlot*2] = key;
        cArray[bucketNum][emptySlot*2+1] = value;
        cArray[bucketNum][0]++;
        return 1;
    }

    public int get(int bucketNum, int key) {
        int i;
        
        if (cArray[bucketNum][0] == 0) {
            return -1;
        }

        for (i = 1; i < NUM_OF_SLOTS; i++) {
            if (cArray[bucketNum][i*2] == key) {
                return cArray[bucketNum][i*2+1];
            }
        }
        return -1;
    }
    
    /**
     * remove key if it exists. Otherwise, ignore it.
     * @param bucketNum
     * @param key
     * @return 1 for success, 0 if the key doesn't exist 
     */
    public int remove(int bucketNum, int key) {
        int i;
        
        if (cArray[bucketNum][0] == 0) {
            return 0;
        }

        for (i = 1; i < NUM_OF_SLOTS; i++) {
            if (cArray[bucketNum][i*2] == key) {
                cArray[bucketNum][i*2] = -1;
                cArray[bucketNum][0]--;
                return 1;
            }
        }
        
        return 0;
    }
}


