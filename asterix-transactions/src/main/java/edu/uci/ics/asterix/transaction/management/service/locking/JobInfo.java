package edu.uci.ics.asterix.transaction.management.service.locking;

import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionManagementConstants.LockManagerConstants.LockMode;

public class JobInfo {
    private EntityInfoManager entityInfoManager;
    private LockWaiterManager lockWaiterManager;
    private TransactionContext jobCtx;
    private int lastHoldingResource; //resource(entity or dataset) which is held by this job lastly
    private int firstWaitingResource; //resource(entity or dataset) which this job is waiting for
    private int upgradingResource; //resource(entity or dataset) which this job is waiting for to upgrade

    //private PrimitiveIntHashMap dLockHT; //used for keeping dataset-granule-lock's count acquired by this job. 

    public JobInfo(EntityInfoManager entityInfoManager, LockWaiterManager lockWaiterManager, TransactionContext txnCtx) {
        this.entityInfoManager = entityInfoManager;
        this.lockWaiterManager = lockWaiterManager;
        this.jobCtx = txnCtx;
        this.lastHoldingResource = -1;
        this.firstWaitingResource = -1;
        this.upgradingResource = -1;
        //this.dLockHT = new PrimitiveIntHashMap(1<<6, 1<<3, 180000);
    }

    public void addHoldingResource(int resource) {

        if (LockManager.IS_DEBUG_MODE) {
            if (entityInfoManager.getJobId(resource) != jobCtx.getJobId().getId()) {
                throw new IllegalStateException("JobInfo(" + jobCtx.getJobId().getId() + ") has diffrent Job(JID:"
                        + entityInfoManager.getJobId(resource) + "'s resource!!!");
            }
            //System.out.println(Thread.currentThread().getName()+"\tJobInfo_AddHolder:"+ resource);
        }

        if (lastHoldingResource != -1) {
            entityInfoManager.setNextJobResource(lastHoldingResource, resource);
        }
        entityInfoManager.setPrevJobResource(resource, lastHoldingResource);
        entityInfoManager.setNextJobResource(resource, -1);
        lastHoldingResource = resource;

        //increaseDatasetLockCount(resource);
    }

    public void removeHoldingResource(int resource) {
        int current = lastHoldingResource;
        int prev;
        int next;

        if (LockManager.IS_DEBUG_MODE) {
            if (entityInfoManager.getJobId(resource) != jobCtx.getJobId().getId()) {
                throw new IllegalStateException("JobInfo(" + jobCtx.getJobId().getId() + ") has diffrent Job(JID:"
                        + entityInfoManager.getJobId(resource) + "'s resource!!!");
            }
            //System.out.println(Thread.currentThread().getName()+"\tJobInfo_RemoveHolder:"+ resource);
        }

        while (current != resource) {

            if (LockManager.IS_DEBUG_MODE) {
                if (current == -1) {
                    //shouldn't occur: debugging purpose
                    try {
                        throw new Exception();
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }

            current = entityInfoManager.getPrevJobResource(current);
        }

        prev = entityInfoManager.getPrevJobResource(current);
        next = entityInfoManager.getNextJobResource(current);
        //update prev->next = next
        if (prev != -1) {
            entityInfoManager.setNextJobResource(prev, next);
        }
        if (next != -1) {
            entityInfoManager.setPrevJobResource(next, prev);
        }
        if (lastHoldingResource == resource) {
            lastHoldingResource = prev;
        }

        //decreaseDatasetLockCount(resource);
    }

    public void addWaitingResource(int waiterObjId) {
        int lastObjId;
        LockWaiter lastObj = null;

        if (firstWaitingResource != -1) {
            //find the lastWaiter
            lastObjId = firstWaitingResource;
            while (lastObjId != -1) {
                lastObj = lockWaiterManager.getLockWaiter(lastObjId);
                if (LockManager.IS_DEBUG_MODE) {
                    int entityInfo = lastObj.getEntityInfoSlot();
                    if (entityInfoManager.getJobId(entityInfo) != jobCtx.getJobId().getId()) {
                        throw new IllegalStateException("JobInfo(" + jobCtx.getJobId().getId()
                                + ") has diffrent Job(JID:" + entityInfoManager.getJobId(entityInfo) + "'s resource!!!");
                    }
                }
                lastObjId = lastObj.getNextWaitingResourceObjId();
            }
            //last->next = new_waiter
            lastObj.setNextWaitingResourceObjId(waiterObjId);
        } else {
            firstWaitingResource = waiterObjId;
        }
        //new_waiter->next = -1
        lastObj = lockWaiterManager.getLockWaiter(waiterObjId);
        if (LockManager.IS_DEBUG_MODE) {
            int entityInfo = lastObj.getEntityInfoSlot();
            if (entityInfoManager.getJobId(entityInfo) != jobCtx.getJobId().getId()) {
                throw new IllegalStateException("JobInfo(" + jobCtx.getJobId().getId() + ") has diffrent Job(JID:"
                        + entityInfoManager.getJobId(entityInfo) + "'s resource!!!");
            }
        }
        lastObj.setNextWaitingResourceObjId(-1);

        //        if (LockManager.IS_DEBUG_MODE) {
        //            System.out.println(Thread.currentThread().getName()+"\tJobInfo_AddWaiter:"+ waiterObjId + ", FirstWaiter:"+firstWaitingResource);            
        //        }
    }

    public void removeWaitingResource(int waiterObjId) {
        int currentObjId = firstWaitingResource;
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
            currentObjId = prevObj.getNextWaitingResourceObjId();
        }

        //get current waiter object
        currentObj = lockWaiterManager.getLockWaiter(currentObjId);

        if (LockManager.IS_DEBUG_MODE) {
            int entityInfo = currentObj.getEntityInfoSlot();
            if (entityInfoManager.getJobId(entityInfo) != jobCtx.getJobId().getId()) {
                throw new IllegalStateException("JobInfo(" + jobCtx.getJobId().getId() + ") has diffrent Job(JID:"
                        + entityInfoManager.getJobId(entityInfo) + "'s resource!!!");
            }
        }

        //get next waiterObjId
        nextObjId = currentObj.getNextWaitingResourceObjId();

        if (prevObjId != -1) {
            //prev->next = next
            prevObj.setNextWaitingResourceObjId(nextObjId);
        } else {
            //removed first waiter. firstWaiter = current->next
            firstWaitingResource = nextObjId;
        }

        //        if (LockManager.IS_DEBUG_MODE) {
        //            System.out.println(Thread.currentThread().getName()+"\tJobInfo_RemoveWaiter:"+ waiterObjId + ", FirstWaiter:"+firstWaitingResource);            
        //        }
    }

    /**********************************************************************************
     * public void increaseDatasetLockCount(int entityInfo) {
     * int datasetId = entityInfoManager.getDatasetId(entityInfo);
     * int count = dLockHT.get(datasetId);
     * if (count == -1) {
     * dLockHT.upsert(datasetId, 1);
     * } else {
     * dLockHT.upsert(datasetId, count+1);
     * }
     * }
     * public void decreaseDatasetLockCount(int entityInfo) {
     * int datasetId = entityInfoManager.getDatasetId(entityInfo);
     * int count = dLockHT.get(datasetId);
     * if (count > 1) {
     * dLockHT.upsert(datasetId, count-1);
     * } else if (count == 1) {
     * dLockHT.remove(datasetId);
     * } else if (count <= 0 ) {
     * throw new IllegalStateException("Illegal state of datasetLock count in JobInfo's dLockHT");
     * }
     * }
     * public boolean isDatasetLockGranted(int datasetId) {
     * return dLockHT.get(datasetId) == -1 ? false : true;
     * }
     **********************************************************************************/

    public boolean isDatasetLockGranted(int datasetId, byte lockMode) {
        int entityInfo = lastHoldingResource;
        byte datasetLockMode;

        while (entityInfo != -1) {
            datasetLockMode = entityInfoManager.getDatasetLockMode(entityInfo);
            datasetLockMode = entityInfoManager.getPKHashVal(entityInfo) == -1 ? datasetLockMode
                    : datasetLockMode == LockMode.S ? LockMode.IS : LockMode.IX;
            if (entityInfoManager.getDatasetId(entityInfo) == datasetId
                    && isStrongerOrEqualToLockMode(datasetLockMode, lockMode)) {
                return true;
            }
            entityInfo = entityInfoManager.getPrevJobResource(entityInfo);
        }
        return false;
    }

    //check whether LockMode modeA is stronger than or equal to LockMode modeB
    private boolean isStrongerOrEqualToLockMode(byte modeA, byte modeB) {
        switch (modeB) {
            case LockMode.X:
                return modeA == LockMode.X;

            case LockMode.IX:
                return modeA == LockMode.IX || modeA == LockMode.X;

            case LockMode.S:
                return modeA == LockMode.S || modeA == LockMode.X;

            case LockMode.IS:
                return true;

            default:
                throw new IllegalStateException("Unsupported dataset lock mode.");
        }
    }

    public String printHoldingResource () {
        StringBuilder s = new StringBuilder();
        int entityInfo = lastHoldingResource;

        while (entityInfo != -1) {
            s.append("entityInfo[").append(entityInfo).append("] ");
            s.append(entityInfoManager.getJobId(entityInfo)).append(" ");
            s.append(entityInfoManager.getDatasetId(entityInfo)).append(" ");
            s.append(entityInfoManager.getPKHashVal(entityInfo)).append(" ");
            s.append(entityInfoManager.getDatasetLockMode(entityInfo)).append(" ");
            s.append(entityInfoManager.getDatasetLockCount(entityInfo)).append(" ");
            s.append(entityInfoManager.getEntityLockCount(entityInfo)).append(" ");
            s.append(entityInfoManager.getEntityLockMode(entityInfo)).append(" ");
            s.append("\n");
            entityInfo = entityInfoManager.getPrevJobResource(entityInfo);
        }
        return s.toString();
    }
    
    /////////////////////////////////////////////////////////
    //  set/get method for private variable
    /////////////////////////////////////////////////////////
    public void setlastHoldingResource(int resource) {
        lastHoldingResource = resource;
    }

    public int getLastHoldingResource() {
        return lastHoldingResource;
    }

    public void setFirstWaitingResource(int resource) {
        firstWaitingResource = resource;
    }

    public int getFirstWaitingResource() {
        return firstWaitingResource;
    }

    public void setUpgradingResource(int resource) {
        upgradingResource = resource;
    }

    public int getUpgradingResource() {
        return upgradingResource;
    }
}
