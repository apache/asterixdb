package edu.uci.ics.asterix.transaction.management.service.locking;

import java.util.ArrayList;
import java.util.Iterator;

import edu.uci.ics.asterix.transaction.management.exception.ACIDException;

/**
 * @author pouria An instance shows information on a "single resource" about
 *         1) current granted locks on the resource to all transactions 2) locks
 *         that are being waiting on by all converting transactions 3) locks
 *         that are being waiting on by all regular waiting transactions
 *         Each lock mode is interpreted as an integer, and it has a
 *         corresponding bit in the mask variable mask variable should be
 *         interpreted as a sequence of bits, where the i-th bit is 1, if and
 *         only if some transaction(s) have a lock of mode i on this resource
 *         counter is an array which has an entry for each lock mode, and its
 *         i-th entry shows the total number of locks of mode i, granted to all
 *         transactions
 */

public class LockInfo {
    final int NUM_OF_LOCK_MODES = 32;
    final int TX_ARRAY_SIZE = 50;
    final int EOL = -1;

    public static final int NOT_FOUND = -2;
    public static final int UNKNOWN_IX = -3;
    public static final int ANY_LOCK_MODE = -4;
    public static final int UNKNOWN_LOCK_MODE = -5;

    private int mask; // i-th bit corresponds to the i-th lock mode
    private int[] counter; // i-th entry shows total num of granted locks of
                           // mode i

    private ArrayList<Integer> grantedList; // (contains index of entries, in
                                            // the txId, mode and counter lists)
    private ArrayList<WaitingInfo> convertList; // Waiting Converters
    private ArrayList<WaitingInfo> waitList; // Regular Waiters

    int nextFreeIx; // Head of free entries lists
    private ArrayList<long[]> txIdList; // i-th entry shows the id of the
                                        // granted/waiting transactions
    private ArrayList<int[]> modeList; // i-th entry shows the mode of the
                                       // granted/waiting-on lock
    private ArrayList<int[]> counterList; // i-th entry shows the number of
                                          // locks (with the defined mode) a
                                          // transaction has taken (In a free
                                          // entry is used as the next ptr (next
                                          // free entry))

    public LockInfo() {
        this.mask = 0;
        this.counter = new int[NUM_OF_LOCK_MODES];
        this.grantedList = new ArrayList<Integer>();
        this.waitList = new ArrayList<WaitingInfo>();
        this.convertList = new ArrayList<WaitingInfo>();
        nextFreeIx = 0;
        this.txIdList = new ArrayList<long[]>();
        txIdList.add(new long[TX_ARRAY_SIZE]);
        this.modeList = new ArrayList<int[]>();
        modeList.add(new int[TX_ARRAY_SIZE]);
        this.counterList = new ArrayList<int[]>();
        counterList.add(initArray(0));
    }

    private int[] initArray(int ixToStart) {
        int[] n = new int[TX_ARRAY_SIZE];
        for (int i = 0; i < TX_ARRAY_SIZE - 1; i++) { // Initializing a new set
                                                      // of entries, attaching
                                                      // them
            n[i] = (++ixToStart); // to the chain of free entries
        }
        n[TX_ARRAY_SIZE - 1] = EOL;
        return n;
    }

    /**
     * @param txId
     * @param lMode
     * @return the index of the entry corresponding to the transaction with the
     *         specified granted lock
     * @throws ACIDException
     */
    public int findInGrantedList(long txId, int lMode) throws ACIDException {
        for (int i : grantedList) {
            if ((getTxId(i) == txId) && ((lMode == ANY_LOCK_MODE) || (lMode == getLockMode(i)))) {
                return i;
            }
        }
        return NOT_FOUND;
    }

    /**
     * @param txId
     * @param lMode
     * @return the index of the entry corresponding to the transaction which is
     *         waiting (as a converter) for the specified lock
     * @throws ACIDException
     */
    public int findInConvertList(long txId, int lMode) throws ACIDException {
        for (WaitingInfo wi : convertList) {
            int i = wi.getWaitingEntry().getIX();
            if ((getTxId(i) == txId) && ((lMode == ANY_LOCK_MODE) || (lMode == getLockMode(i)))) {
                return i;
            }
        }
        return NOT_FOUND;
    }

    /**
     * @param txId
     * @param lMode
     * @return the index of the entry corresponding to the transaction which is
     *         waiting (as a regular waiter) for the specified lock
     * @throws ACIDException
     */
    public int findInWaitList(long txId, int lMode) throws ACIDException {
        for (WaitingInfo wi : waitList) {
            int i = wi.getWaitingEntry().getIX();
            if ((getTxId(i) == txId) && ((lMode == ANY_LOCK_MODE) || (lMode == getLockMode(i)))) {
                return i;
            }
        }
        return NOT_FOUND;
    }

    /**
     * @param txId
     * @param lMode
     * @return the object, on which the specified transaction is waiting for the
     *         specified lock
     * @throws ACIDException
     */
    public WaitingInfo getWaitingOnObject(long txId, int lMode) throws ACIDException {
        WaitingInfo wObj = null;
        Iterator<WaitingInfo> cIt = convertList.iterator();
        while (cIt.hasNext()) {
            wObj = cIt.next();
            int ix = wObj.getWaitingEntry().getIX();
            if ((getTxId(ix) == txId) && ((lMode == ANY_LOCK_MODE) || (lMode == getLockMode(ix)))) {
                return wObj;
            }
        }

        Iterator<WaitingInfo> wIt = waitList.iterator();
        while (wIt.hasNext()) {
            wObj = wIt.next();
            int ix = wObj.getWaitingEntry().getIX();
            if ((getTxId(ix) == txId) && ((lMode == ANY_LOCK_MODE) || (lMode == getLockMode(ix)))) {
                return wObj;
            }
        }
        throw new ACIDException("Waiting Entry for transaction " + txId + " Could not be found");
    }

    public Iterator<WaitingInfo> getIteratorOnConverter() {
        return (convertList.iterator());
    }

    public Iterator<WaitingInfo> getIteratorOnWaiters() {
        return (waitList.iterator());
    }

    /**
     * @param txId
     * @param lMode
     * @param eix
     *            index of the entry corresponding to the transaction, its
     *            granted lock and its counter
     * @throws ACIDException
     */
    public void addToGranted(long txId, int lMode, int eix) throws ACIDException {
        if (eix == UNKNOWN_IX) {
            eix = findInGrantedList(txId, lMode);
        }
        if (eix == NOT_FOUND) { // new lock of mode lMode for Txr
            int ix = allocateEntryForRequest();
            grantedList.add(ix);
            setTxId(txId, ix);
            setLockMode(lMode, ix);
            setReqCount(1, ix);
            mask |= (0x01 << lMode);
        } else { // Redundant lock of mode lMode for Txr
            incReqCount(eix);
        }
        counter[lMode]++;
    }

    /**
     * @param txId
     * @param lMode
     * @param eix
     *            index of the entry corresponding to the transaction, its
     *            granted lock and its counter
     * @throws ACIDException
     */
    public void removeFromGranted(long txId, int lMode, int eix) throws ACIDException {
        removeFromGranted(txId, lMode, true, eix);
    }

    /**
     * @param txId
     * @param lMode
     * @param forced
     *            whether to remove all the locks, with the given mode, grabbed
     *            by the transaction or consider the counter (removing just one
     *            lock in case the transaction has several locks with the
     *            specified mode)
     * @param eix
     *            index of the entry corresponding to the transaction, its
     *            granted lock and its counter
     * @throws ACIDException
     */
    private void removeFromGranted(long txId, int lMode, boolean forced, int eix) throws ACIDException {
        if (eix == UNKNOWN_IX) {
            eix = findInGrantedList(txId, lMode);
            if (eix == NOT_FOUND) {
                return;
            }
        }

        if (lMode == ANY_LOCK_MODE) {
            lMode = getLockMode(eix);
        }

        int count = getReqCount(eix);
        if (!forced) {
            if (count > 1) {
                setReqCount((count - 1), eix);
                counter[lMode]--;
                return;
            }
        }
        // forced or count is 1
        grantedList.remove((new Integer(eix)));
        freeEntry(eix);
        counter[lMode] -= count;
        if (counter[lMode] == 0) { // No one else has lock with this mode
            mask &= (~(0x00 | (0x01 << lMode)));
        }
    }

    /**
     * @param txId
     * @param lMode
     * @param entry
     *            the object, specified transaction is going to wait on
     * @throws ACIDException
     */
    public void addToConvert(long txId, int lMode, WaitEntry entry) throws ACIDException {
        int eix = findInConvertList(txId, lMode);
        if (eix == NOT_FOUND) {
            int ix = allocateEntryForRequest();
            entry.setIx(ix);
            entry.setForWait();
            convertList.add(new WaitingInfo(entry));
            setTxId(txId, ix);
            setLockMode(lMode, ix);
            setReqCount(1, ix);
        } else {
            throw new ACIDException("Adding an already existing converter");
        }
    }

    /**
     * @param txId
     * @param lMode
     * @param eix
     *            index of the entry corresponding to the transaction in the
     *            converters list
     * @throws ACIDException
     */
    public void prepareToRemoveFromConverters(long txId, int lMode, int eix) throws ACIDException {
        prepareToRemoveFromConverters(txId, lMode, true, eix);
    }

    /**
     * @param txId
     * @param lMode
     * @param forced
     *            whether to ignore the counter and remove the transaction from
     *            the converters list or consider the request counter
     * @param eix
     *            index of the entry corresponding to the transaction in the
     *            converters list
     * @throws ACIDException
     */
    private void prepareToRemoveFromConverters(long txId, int lMode, boolean forced, int eix) throws ACIDException {
        if (eix == UNKNOWN_IX) {
            eix = findInConvertList(txId, lMode);
            if (eix == NOT_FOUND) {
                throw new ACIDException("Lock entry not found in the waiting list");
            }
        }
        freeEntry(eix);
    }

    /**
     * @param txId
     * @return the object specified transaction is waiting on for conversion
     * @throws ACIDException
     */
    public WaitEntry removeFromConverters(long txId) throws ACIDException {
        Iterator<WaitingInfo> it = convertList.iterator();
        while (it.hasNext()) {
            WaitingInfo next = it.next();
            if (getTxId(next.getWaitingEntry().getIX()) == txId) {
                it.remove();
                return next.getWaitingEntry();
            }
        }
        return null;
    }

    /**
     * @param txId
     * @param lMode
     * @param entry
     * @throws ACIDException
     */
    public void addToWaiters(long txId, int lMode, WaitEntry entry) throws ACIDException {
        int ix = allocateEntryForRequest();
        entry.setIx(ix);
        entry.setForWait();
        waitList.add(new WaitingInfo(entry));
        setTxId(txId, ix);
        setLockMode(lMode, ix);
        setReqCount(1, ix);
    }

    public void prepareToRemoveFromWaiters(long txId, int lMode, int eix) throws ACIDException {
        prepareToRemoveFromWaiters(txId, lMode, true, eix);
    }

    /**
     * Removes and recycles the entry containing the information about the
     * transaction, its lock mode and the counter
     * 
     * @param txId
     * @param lMode
     * @param forced
     * @param eix
     *            index of the entry, needs to be freed
     * @throws ACIDException
     */
    private void prepareToRemoveFromWaiters(long txId, int lMode, boolean forced, int eix) throws ACIDException {
        if (eix == UNKNOWN_IX) {
            eix = findInWaitList(txId, lMode);
            if (eix == NOT_FOUND) {
                throw new ACIDException("Lock entry not found in the waiting list");
            }
        }
        freeEntry(eix);
    }

    /**
     * @param txId
     * @return the object the transaction is waiting on (as a regular waiter)
     * @throws ACIDException
     */
    public WaitEntry removeFromWaiters(long txId) throws ACIDException {
        Iterator<WaitingInfo> it = waitList.iterator();
        while (it.hasNext()) {
            WaitingInfo next = it.next();
            if (getTxId(next.getWaitingEntry().getIX()) == txId) {
                it.remove();
                return next.getWaitingEntry();
            }
        }
        return null;
    }

    /**
     * @param lMode
     * @param eix
     *            index of the entry corresponding to the transaction's lock and
     *            its counter
     */
    public void grantRedundantLock(int lMode, int eix) {
        incReqCount(eix);
        counter[lMode]++;
    }

    /**
     * @param txId
     * @param eix
     *            index of the entry corresponding to the transaction
     * @return the actual lock mode, granted to the specified transaction
     * @throws ACIDException
     */
    public int getGrantedLockMode(long txId, int eix) throws ACIDException {
        if (eix != UNKNOWN_IX) {
            return getLockMode(eix);
        }
        int ix = findInGrantedList(txId, ANY_LOCK_MODE);
        if (ix == NOT_FOUND) {
            return UNKNOWN_LOCK_MODE;
        }
        return getLockMode(ix);
    }

    /**
     * @param txId
     * @param eix
     *            index of the entry corresponding to the transaction
     * @return the actual lock mode, the specified transaction is waiting to
     *         convert to
     * @throws ACIDException
     */
    public int getConvertLockMode(long txId, int eix) throws ACIDException {
        if (eix != UNKNOWN_IX) {
            return getLockMode(eix);
        }
        int ix = findInConvertList(txId, ANY_LOCK_MODE);
        if (ix == NOT_FOUND) {
            return UNKNOWN_LOCK_MODE;
        }
        return getLockMode(ix);
    }

    /**
     * @param txId
     * @param eix
     *            index of the entry corresponding to the transaction
     * @return the actual lock mode, the specified transaction is waiting to
     *         grab
     * @throws ACIDException
     */
    public int getWaitLockMode(long txId, int eix) throws ACIDException {
        if (eix != UNKNOWN_IX) {
            return getLockMode(eix);
        }
        int ix = findInWaitList(txId, ANY_LOCK_MODE);
        if (ix == NOT_FOUND) {
            return UNKNOWN_LOCK_MODE;
        }
        return getLockMode(ix);
    }

    public boolean isConvertListEmpty() {
        return (!(convertList.size() > 0));
    }

    public int getMask() {
        return mask;
    }

    /**
     * @param txId
     * @param lMode
     * @param eix
     *            index of the entry corresponding to the transaction's
     *            currently grabbed lock
     * @return the updated as if the granted lock to the specified transaction
     *         gets removed from it (Mainly used to exclude self-conflicts when
     *         checking for conversions)
     * @throws ACIDException
     */
    public int getUpdatedMask(long txId, int lMode, int eix) throws ACIDException {
        if (eix == UNKNOWN_IX) {
            eix = findInGrantedList(txId, lMode);
        }
        if (eix == NOT_FOUND) {
            return mask;
        }

        int txCount = getReqCount(eix);
        int totalCount = getLockAggCounter(lMode);

        if (totalCount == txCount) { // txId is the only lock-holder with this
                                     // mode
            return (mask & (~(0x00 | (0x01 << lMode))));
        }

        return mask;
    }

    /**
     * @param lmix
     * @return the total number of locks of the specified mode, grabbed on this
     *         resource (by all transactions)
     */
    private int getLockAggCounter(int lmix) {
        return counter[lmix];
    }

    /**
     * Populates the grantedIDs list with the ids of all transactions in the
     * granted list
     * 
     * @param grantedIDs
     */
    public void getGrantedListTxIDs(ArrayList<Long> grantedIDs) {
        Iterator<Integer> gIt = grantedList.iterator();
        while (gIt.hasNext()) {
            grantedIDs.add(getTxId(gIt.next()));
        }
    }

    /**
     * @return the index of an entry that can be used to capture one transaction
     *         and its requested/granted lock mode and the counter
     */
    private int allocateEntryForRequest() {
        if (nextFreeIx == EOL) {
            nextFreeIx = txIdList.size() * TX_ARRAY_SIZE;
            txIdList.add(new long[TX_ARRAY_SIZE]);
            modeList.add(new int[TX_ARRAY_SIZE]);
            counterList.add(initArray(nextFreeIx));
        }
        int ixToRet = nextFreeIx;
        nextFreeIx = getReqCount(nextFreeIx);
        return ixToRet;
    }

    /**
     * @param ix
     *            index of the entry, to be recycled
     */
    private void freeEntry(int ix) {
        setReqCount(nextFreeIx, ix); // counter holds ptr to next free entry in
                                     // free entries
        nextFreeIx = ix;
    }

    /**
     * @param ix
     *            index of the entry that captures the transaction id
     * @return id of the transaction whose info is captured in the specified
     *         index
     */
    public long getTxId(int ix) {
        return (txIdList.get(ix / TX_ARRAY_SIZE)[ix % TX_ARRAY_SIZE]);
    }

    /**
     * @param txId
     * @param ix
     *            index of the entry that will capture the transaction id
     */
    private void setTxId(long txId, int ix) {
        txIdList.get(ix / TX_ARRAY_SIZE)[ix % TX_ARRAY_SIZE] = txId;
    }

    /**
     * @param ix
     *            index of the entry that captures the lock mode
     *            requested/grabbed by the specified transaction
     * @return
     */
    public int getLockMode(int ix) {
        return (modeList.get(ix / TX_ARRAY_SIZE)[ix % TX_ARRAY_SIZE]);
    }

    /**
     * @param lMode
     * @param index
     *            of the entry that will capture the lock mode requested/grabbed
     *            by the specified transaction
     */
    private void setLockMode(int lMode, int ix) {
        modeList.get(ix / TX_ARRAY_SIZE)[ix % TX_ARRAY_SIZE] = lMode;
    }

    /**
     * @param ix
     * @return index of the entry that captures the counter of locks
     */
    public int getReqCount(int ix) {
        return (counterList.get(ix / TX_ARRAY_SIZE)[ix % TX_ARRAY_SIZE]);
    }

    /**
     * @param count
     * @param ix
     *            index of the entry that captures the counter of locks
     */
    private void setReqCount(int count, int ix) {
        counterList.get(ix / TX_ARRAY_SIZE)[ix % TX_ARRAY_SIZE] = count;
    }

    /**
     * @param ix
     *            index of the counter, needed to be incremented on behalf of a
     *            transaction
     */
    private void incReqCount(int ix) {
        counterList.get(ix / TX_ARRAY_SIZE)[ix % TX_ARRAY_SIZE]++;
    }
}

class WaitingInfo {
    /**
     * An object of this class captures the information corresponding to a
     * regular or converter waiter
     */

    private boolean isVictim; // Whether the corresponding transaction is an
                              // Victim or it can be waken up safely
    private WaitEntry waitEntry; // The object, on which the waiter is waiting.
                                 // This object is mainly used to notify the
                                 // waiter, to be waken up

    public WaitingInfo(WaitEntry waitEntry) {
        this.waitEntry = waitEntry;
        this.isVictim = false;
    }

    public boolean isVictim() {
        return isVictim;
    }

    public void setAsVictim() {
        this.isVictim = true;
    }

    public WaitEntry getWaitingEntry() {
        return this.waitEntry;
    }
}