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

/**
 * LockWaiter object is used for keeping a lock waiter or a lock upgrader information on a certain resource.
 * The resource can be a dataset or an entity. 
 * @author kisskys
 *
 */
public class LockWaiter {
    /**
     * entityInfoSlotNum:
     * If this LockWaiter object is used, this variable is used to indicate the corresponding EntityInfoSlotNum.
     * Otherwise, the variable is used for nextFreeSlot Which indicates the next free waiter object.
     */
    private int entityInfoSlotNum;
    private boolean wait;
    private boolean victim;
    private byte waiterCount;
    private boolean firstGetUp;
    private int nextWaiterObjId; //used for DatasetLockInfo and EntityLockInfo
    private int nextWaitingResourceObjId; //used for JobInfo
    private long beginWaitTime;
    private boolean isWaiter; //is upgrader or waiter
    private boolean isWaitingOnEntityLock; //is waiting on datasetLock or entityLock

    public LockWaiter() {
        this.victim = false;
        this.wait = true;
        waiterCount = 0;
        nextWaiterObjId = -1;
        nextWaitingResourceObjId = -1;
    }

    public void setEntityInfoSlot(int slotNum) {
        this.entityInfoSlotNum = slotNum;
    }

    public int getEntityInfoSlot() {
        return this.entityInfoSlotNum;
    }

    public void setNextFreeSlot(int slotNum) {
        this.entityInfoSlotNum = slotNum;
    }

    public int getNextFreeSlot() {
        return this.entityInfoSlotNum;
    }

    public void setWait(boolean wait) {
        this.wait = wait;
    }

    public boolean needWait() {
        return this.wait;
    }

    public void setVictim(boolean victim) {
        this.victim = victim;
    }

    public boolean isVictim() {
        return this.victim;
    }
    
    public void increaseWaiterCount() {
        waiterCount++;
    }
    
    public void decreaseWaiterCount() {
        waiterCount--;
    }
    
    public byte getWaiterCount() {
        return waiterCount;
    }
    
    public void setWaiterCount(byte count) {
        waiterCount = count;
    }
    
    public void setFirstGetUp(boolean isFirst) {
        firstGetUp = isFirst;
    }
    
    public boolean isFirstGetUp() {
        return firstGetUp;
    }
    
    public void setNextWaiterObjId(int next) {
        nextWaiterObjId = next;
    }
    
    public int getNextWaiterObjId() {
        return nextWaiterObjId;
    }
    
    public void setNextWaitingResourceObjId(int next) {
        nextWaitingResourceObjId = next;
    }
    
    public int getNextWaitingResourceObjId() {
        return nextWaitingResourceObjId;
    }
    
    public void setBeginWaitTime(long time) {
        this.beginWaitTime = time;
    }
    
    public long getBeginWaitTime() {
        return beginWaitTime;
    }
    
    public boolean isWaiter() {
        return isWaiter;
    }
    
    public void setWaiter(boolean isWaiter) {
        this.isWaiter = isWaiter;
    }
    
    public boolean isWaitingOnEntityLock() {
        return isWaitingOnEntityLock;
    }
    
    public void setWaitingOnEntityLock(boolean isWaitingOnEntityLock) {
        this.isWaitingOnEntityLock = isWaitingOnEntityLock;
    }
    
}
