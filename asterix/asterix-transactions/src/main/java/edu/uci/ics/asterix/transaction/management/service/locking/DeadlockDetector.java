package edu.uci.ics.asterix.transaction.management.service.locking;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author pouria Performing a DFS search, upon adding each waiter to a waiting
 *         list to avoid deadlocks this class implements such a loop-detector in
 *         the wait-for-graph
 */

public class DeadlockDetector {
    LockManager lockMgr;

    ArrayList<Long> grantedList;
    ArrayList<Long> nextTxrIDs;
    ArrayList<Long> visited;
    ArrayList<Long> nextGrantedTxIDs;

    public DeadlockDetector(LockManager lm) {
        this.lockMgr = lm;
        this.grantedList = new ArrayList<Long>();
        this.nextTxrIDs = new ArrayList<Long>();
        this.visited = new ArrayList<Long>();
        this.nextGrantedTxIDs = new ArrayList<Long>();
    }

    public synchronized boolean isSafeToAdd(long reqTxId, byte[] resourceId) {
        grantedList.clear();
        lockMgr.getLockInfo(resourceId).getGrantedListTxIDs(grantedList);
        visited.clear();
        while (grantedList.size() > 0) { // Doing a DFS for loop detection
            nextTxrIDs.clear();
            for (long grantee : grantedList) {
                TxrInfo nextTInfoList = lockMgr.getTxrInfo(grantee);
                if (nextTInfoList == null) {
                    continue;
                }
                byte[] nextWaitOnRid = nextTInfoList.getWaitOnRid();
                if (nextWaitOnRid == null) {
                    continue;
                }
                nextGrantedTxIDs.clear();
                lockMgr.getLockInfo(nextWaitOnRid).getGrantedListTxIDs(nextGrantedTxIDs);
                if (nextGrantedTxIDs.contains(reqTxId)) {
                    return false;
                }
                removeVisitedTxIDs();
                nextTxrIDs.addAll(nextGrantedTxIDs);
                visited.add(grantee);
            }
            grantedList.clear();
            grantedList.addAll(nextTxrIDs);
        }
        return true;
    }

    private void removeVisitedTxIDs() {
        Iterator<Long> txIdIt = nextGrantedTxIDs.iterator();
        while (txIdIt.hasNext()) {
            if (visited.contains(txIdIt.next())) {
                txIdIt.remove();
            }
        }
    }

}
