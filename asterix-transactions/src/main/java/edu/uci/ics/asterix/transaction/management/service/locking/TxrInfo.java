package edu.uci.ics.asterix.transaction.management.service.locking;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext;

/**
 * @author pouria An instance shows information about all the locks a specific
 *         transaction is holding and/or is waiting on (whether for conversion
 *         or as a regular waiter) (Each TInfo instance in the infoList captures
 *         information about one lock on one resource)
 *         If the transaction is waiting for a lock on a specific resource, the
 *         ID of that resource is captured in waitingOnRid
 */

public class TxrInfo {
    public static final int NOT_FOUND = -2;
    public static final int NOT_KNOWN_IX = -3;

    private ArrayList<TInfo> infoList;
    private byte[] waitingOnRid;
    private TransactionContext context;

    public TxrInfo(TransactionContext context) {
        this.context = context;
        this.infoList = new ArrayList<TInfo>();
        this.waitingOnRid = null;
    }

    public TInfo getTxrInfo(byte[] resourceId, int lMode, int eix) {
        if (eix == NOT_KNOWN_IX) {
            eix = findInList(resourceId, lMode);
        }

        if (eix != NOT_FOUND) {
            return infoList.get(eix);
        }
        return null;
    }

    public void addGrantedLock(byte[] resourceId, int lMode) {
        int eix = findInList(resourceId, lMode);
        if (eix == NOT_FOUND) { // We do not add a redundant lock here
            infoList.add(new TInfo(resourceId, lMode));
        }
    }

    public void removeLock(byte[] resourceId, int lMode, int eix) {
        if (eix == NOT_KNOWN_IX) {
            eix = findInList(resourceId, lMode);
        }
        if (eix != NOT_FOUND) {
            infoList.remove(eix);
        }
    }

    public TransactionContext getContext() {
        return context;
    }

    public void setWaitOnRid(byte[] resourceId) {
        this.waitingOnRid = null;
        if (resourceId != null) {
            this.waitingOnRid = Arrays.copyOf(resourceId, resourceId.length);
        }

    }

    public byte[] getWaitOnRid() {
        return this.waitingOnRid;
    }

    public long getStartWaitTime() {
        return this.context.getStartWaitTime();
    }

    public int getSize() {
        return infoList.size();
    }

    public int findInList(byte[] resourceId, int lMode) {
        for (int i = 0; i < infoList.size(); i++) {
            TInfo ti = infoList.get(i);
            if (((lMode == LockInfo.ANY_LOCK_MODE) || (lMode == ti.getMode()))
                    && Arrays.equals(ti.getResourceId(), resourceId)) {
                return i;
            }
        }
        return NOT_FOUND;
    }

    public Iterator<TInfo> getIterator() { // TODO change the direct way of
        // accessing
        return infoList.iterator();
    }
}

class TInfo {
    private byte[] resourceId; // The resource on which the lock is held or is
                               // waiting to be held
    private int lockMode; // The granted/waiting-for lockMode

    public TInfo(byte[] rId, int lMode) {
        this.resourceId = rId;
        this.lockMode = lMode;
    }

    public byte[] getResourceId() {
        return this.resourceId;
    }

    public int getMode() {
        return lockMode;
    }

    public void setMode(int mode) {
        lockMode = mode;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof TInfo)) {
            return false;
        }
        TInfo t = (TInfo) o;
        return ((t.lockMode == lockMode) && (Arrays.equals(t.resourceId, resourceId)));
    }
}
