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

import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionManagementConstants.LockManagerConstants.LockMode;

public class DatasetLockInfo {
    private EntityLockInfoManager entityLockInfoManager;
    private EntityInfoManager entityInfoManager;
    private LockWaiterManager lockWaiterManager;
    private PrimitiveIntHashMap entityResourceHT;
    private int IXCount;
    private int ISCount;
    private int XCount;
    private int SCount;
    private int lastHolder;
    private int firstWaiter;
    private int firstUpgrader;

    public DatasetLockInfo(EntityLockInfoManager entityLockInfoManager, EntityInfoManager entityInfoManager,
            LockWaiterManager lockWaiterManager) {
        this.entityLockInfoManager = entityLockInfoManager;
        this.entityInfoManager = entityInfoManager;
        this.lockWaiterManager = lockWaiterManager;
        entityResourceHT = new PrimitiveIntHashMap();
        lastHolder = -1; //-1 stands for end of list
        firstWaiter = -1;
        firstUpgrader = -1;
    }

    public void increaseLockCount(byte lockMode) {
        switch (lockMode) {
            case LockMode.IX:
                IXCount++;
                break;
            case LockMode.IS:
                ISCount++;
                break;
            case LockMode.X:
                XCount++;
                break;
            case LockMode.S:
                SCount++;
                break;
            default:
                throw new IllegalStateException("Invalid dataset lock mode");
        }
    }

    public void decreaseLockCount(byte lockMode) {
        switch (lockMode) {
            case LockMode.IX:
                IXCount--;
                break;
            case LockMode.IS:
                ISCount--;
                break;
            case LockMode.X:
                XCount--;
                break;
            case LockMode.S:
                SCount--;
                break;
            default:
                throw new IllegalStateException("Invalid dataset lock mode");
        }
    }

    public void increaseLockCount(byte lockMode, int count) {
        switch (lockMode) {
            case LockMode.IX:
                IXCount += count;
                break;
            case LockMode.IS:
                ISCount += count;
                break;
            case LockMode.X:
                XCount += count;
                break;
            case LockMode.S:
                SCount += count;
                break;
            default:
                throw new IllegalStateException("Invalid dataset lock mode");
        }
    }

    public void decreaseLockCount(byte lockMode, int count) {
        switch (lockMode) {
            case LockMode.IX:
                IXCount -= count;
                break;
            case LockMode.IS:
                ISCount -= count;
                break;
            case LockMode.X:
                XCount -= count;
                break;
            case LockMode.S:
                SCount -= count;
                break;
            default:
                throw new IllegalStateException("Invalid dataset lock mode");
        }
    }

    public boolean isUpgradeCompatible(byte lockMode, int entityInfo) {
        switch (lockMode) {
        //upgrade from IS -> IX
        //XCount is guaranteed to be 0.
        //upgrade is allowed if SCount is 0.
            case LockMode.IX:
                return SCount == 0;

                //upgrade from S -> X
                //XCount and IXCount are guaranteed to be 0.
                //upgrade is allowed if ISCount is 0.
            case LockMode.X:
                return ISCount == 0;

            default:
                throw new IllegalStateException("Invalid upgrade lock mode");
        }
    }

    public boolean isCompatible(byte lockMode) {
        switch (lockMode) {
            case LockMode.IX:
                return SCount == 0 && XCount == 0;

            case LockMode.IS:
                return XCount == 0;

            case LockMode.X:
                return ISCount == 0 && IXCount == 0 && SCount == 0 && XCount == 0;

            case LockMode.S:
                return IXCount == 0 && XCount == 0;

            default:
                throw new IllegalStateException("Invalid upgrade lock mode");
        }
    }

    public int findEntityInfoFromHolderList(int jobId, int hashVal) {
        int entityInfo;
        int eLockInfo;
        int waiterObjId;
        if (hashVal == -1) {//dataset-granule lock
            entityInfo = lastHolder;
            while (entityInfo != -1) {
                if (jobId == entityInfoManager.getJobId(entityInfo)) {
                    return entityInfo;
                }
                entityInfo = entityInfoManager.getPrevEntityActor(entityInfo);
            }
            return -1;
        } else { //entity-granule lock
            eLockInfo = entityResourceHT.get(hashVal);
            if (eLockInfo == -1) {
                return -1;
            }
            entityInfo = entityLockInfoManager.findEntityInfoFromHolderList(eLockInfo, jobId, hashVal);
            if (entityInfo == -1) {
                //find the entityInfo from the waiter list of entityLockInfo. 
                //There is a case where dataset-granule lock is acquired, but entity-granule lock is not acquired yet.
                //In this case, the waiter of the entityLockInfo represents the holder of the datasetLockInfo.
                waiterObjId = entityLockInfoManager.findWaiterFromWaiterList(eLockInfo, jobId, hashVal);
                if (waiterObjId != -1) {
                    entityInfo = lockWaiterManager.getLockWaiter(waiterObjId).getEntityInfoSlot();
                }
            }
            return entityInfo;
        }
    }

    public int findWaiterFromWaiterList(int jobId, int hashVal) {
        int waiterObjId;
        LockWaiter waiterObj;
        int entityInfo = 0;

        waiterObjId = firstWaiter;
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

    public int findUpgraderFromUpgraderList(int jobId, int hashVal) {
        int waiterObjId;
        LockWaiter waiterObj;
        int entityInfo = 0;

        waiterObjId = firstUpgrader;
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

    public boolean isNoHolder() {
        return ISCount == 0 && IXCount == 0 && SCount == 0 && XCount == 0;
    }

    public void addHolder(int holder) {
        entityInfoManager.setPrevEntityActor(holder, lastHolder);
        lastHolder = holder;
    }

    /**
     * Remove holder from linked list of Actor.
     * Also, remove the corresponding resource from linked list of resource
     * in order to minimize JobInfo's resource link traversal.
     * 
     * @param holder
     * @param jobInfo
     */
    public void removeHolder(int holder, JobInfo jobInfo) {
        int prev = lastHolder;
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
            lastHolder = entityInfoManager.getPrevEntityActor(prev);
        }

        //Notice!!
        //remove the corresponding resource from linked list of resource.
        //it is guaranteed that there is no waiter or upgrader in the JobInfo when this function is called.
        prev = entityInfoManager.getPrevJobResource(holder);
        next = entityInfoManager.getNextJobResource(holder);

        if (prev != -1) {
            entityInfoManager.setNextJobResource(prev, next);
        }

        if (next != -1) {
            entityInfoManager.setPrevJobResource(next, prev);
        } else {
            //This entityInfo(i.e., holder) is the last resource held by this job.
            jobInfo.setlastHoldingResource(holder);
        }
        
        //jobInfo.decreaseDatasetLockCount(holder);
    }

    /**
     * append new waiter to the end of waiters
     * 
     * @param waiterObjId
     */
    public void addWaiter(int waiterObjId) {
        int lastObjId;
        LockWaiter lastObj = null;

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
            firstWaiter = waiterObjId;
        }
        //new_waiter->next = -1
        lastObj = lockWaiterManager.getLockWaiter(waiterObjId);
        lastObj.setNextWaiterObjId(-1);

//        if (LockManager.IS_DEBUG_MODE) {
//            System.out.println(printWaiters());
//        }
    }

    public void removeWaiter(int waiterObjId) {
        int currentObjId = firstWaiter;
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
            firstWaiter = nextObjId;
        }

//        if (LockManager.IS_DEBUG_MODE) {
//            System.out.println(printWaiters());
//        }
    }

    public void addUpgrader(int waiterObjId) {
        int lastObjId;
        LockWaiter lastObj = null;

        if (firstUpgrader != -1) {
            //find the lastWaiter
            lastObjId = firstUpgrader;
            while (lastObjId != -1) {
                lastObj = lockWaiterManager.getLockWaiter(lastObjId);
                lastObjId = lastObj.getNextWaiterObjId();
            }
            //last->next = new_waiter
            lastObj.setNextWaiterObjId(waiterObjId);
        } else {
            firstUpgrader = waiterObjId;
        }
        //new_waiter->next = -1
        lastObj = lockWaiterManager.getLockWaiter(waiterObjId);
        lastObj.setNextWaiterObjId(-1);
    }

    public void removeUpgrader(int waiterObjId) {
        int currentObjId = firstUpgrader;
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
            firstUpgrader = nextObjId;
        }
    }

    //debugging method
    public String printWaiters() {
        StringBuilder s = new StringBuilder();
        int waiterObjId;
        LockWaiter waiterObj;
        int entityInfo;

        s.append("WID\tWCT\tEID\tJID\tDID\tPK\n");

        waiterObjId = firstWaiter;
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
    
    public String coreDump() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n\t firstUpgrader: " + firstUpgrader);
        sb.append("\n\t firstWaiter: " + firstWaiter);
        sb.append("\n\t lastHolder: " + lastHolder);
        sb.append("\n\t ISCount: " + ISCount);
        sb.append("\n\t IXCount: " + IXCount);
        sb.append("\n\t SCount: " + SCount);
        sb.append("\n\t XCount: " + XCount);
        sb.append("\n\t entityResourceHT");
        sb.append(entityResourceHT.prettyPrint());
        return sb.toString();
    }

    /////////////////////////////////////////////////////////
    //  set/get method for private variable
    /////////////////////////////////////////////////////////
    public void setIXCount(int count) {
        IXCount = count;
    }

    public int getIXCount() {
        return IXCount;
    }

    public void setISCount(int count) {
        ISCount = count;
    }

    public int getISCount() {
        return ISCount;
    }

    public void setXCount(int count) {
        XCount = count;
    }

    public int getXCount() {
        return XCount;
    }

    public void setSCount(int count) {
        SCount = count;
    }

    public int getSCount() {
        return SCount;
    }

    public void setLastHolder(int holder) {
        lastHolder = holder;
    }

    public int getLastHolder() {
        return lastHolder;
    }

    public void setFirstWaiter(int waiter) {
        firstWaiter = waiter;
    }

    public int getFirstWaiter() {
        return firstWaiter;
    }

    public void setFirstUpgrader(int upgrader) {
        firstUpgrader = upgrader;
    }

    public int getFirstUpgrader() {
        return firstUpgrader;
    }

    public PrimitiveIntHashMap getEntityResourceHT() {
        return entityResourceHT;
    }

}