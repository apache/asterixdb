package edu.uci.ics.asterix.transaction.management.service.locking;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionManagementConstants;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionProvider;

/**
 * @author pouria An implementation of the ILockManager interface for the
 *         specific case of locking protocol with two lock modes: (S) and (X),
 *         where S lock mode is shown by 0, and X lock mode is shown by 1.
 * @see ILockManager, DeadlockDetector, TimeOutDetector, ILockMatrix,
 *      LockMatrix, TxrInfo and LockInfo
 */

public class LockManager implements ILockManager {

    private static final Logger LOGGER = Logger.getLogger(LockManager.class.getName());

    final int INIT_TABLE_SIZE = 50;
    private LMTables lmTables;
    ILockMatrix lMtx;

    WaitObjectManager woManager;
    TimeOutDetector toutDetector;
    DeadlockDetector deadlockDetector;

    public LockManager(TransactionProvider factory) throws ACIDException {
        Properties p = new Properties();
        InputStream is = null;
        ILockMatrix lockMatrix = null;
        int[] confTab = null;
        int[] convTab = null;

        try {
            File file = new File(TransactionManagementConstants.LockManagerConstants.LOCK_CONF_DIR + File.separator
                    + TransactionManagementConstants.LockManagerConstants.LOCK_CONF_FILE);
            if (file.exists()) {
                is = new FileInputStream(TransactionManagementConstants.LockManagerConstants.LOCK_CONF_DIR
                        + File.separator + TransactionManagementConstants.LockManagerConstants.LOCK_CONF_FILE);
                p.load(is);
                confTab = getConfTab(p);
                convTab = getConvTab(p);
            } else {
                if (LOGGER.isLoggable(Level.SEVERE)) {
                    LOGGER.severe("Lock configuration file not found, using defaults !");
                }
                confTab = TransactionManagementConstants.LockManagerConstants.LOCK_CONFLICT_MATRIX;
                convTab = TransactionManagementConstants.LockManagerConstants.LOCK_CONVERT_MATRIX;
            }
            lockMatrix = new LockMatrix(confTab, convTab);
            initialize(lockMatrix);
        } catch (IOException ioe) {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    throw new ACIDException("unable to close input stream ", e);
                }
            }
            throw new ACIDException(" unable to create LockManager", ioe);
        }
    }

    private static int[] getConfTab(Properties properties) {
        return null;
    }

    private static int[] getConvTab(Properties properties) {
        return null;
    }

    private void initialize(ILockMatrix matrix) throws ACIDException {
        this.lmTables = new LMTables(INIT_TABLE_SIZE);
        this.lMtx = matrix;
        woManager = new WaitObjectManager();
        this.deadlockDetector = new DeadlockDetector(this);
        this.toutDetector = new TimeOutDetector(this);
    }

    @Override
    public boolean lock(TransactionContext context, byte[] resourceID, int mode) throws ACIDException {
        long txId = context.getTransactionID();
        TxrInfo txrInfo = null;
        WaitEntry waitObj = null;
        Boolean isConverting = false;
        int grantedMode = -1;
        LockInfo lInfo = null;
        boolean shouldAbort = false;

        synchronized (lmTables) {
            txrInfo = lmTables.getTxrInfo(txId);
            if (txrInfo == null) {
                txrInfo = new TxrInfo(context);
                lmTables.putTxrInfo(txId, txrInfo);
            }

            lInfo = lmTables.getLockInfo(resourceID);
            if (lInfo == null) { // First lock on the resource, grant it
                lInfo = new LockInfo();
                lInfo.addToGranted(txId, mode, LockInfo.NOT_FOUND);
                lmTables.putLockInfo(resourceID, lInfo);
                txrInfo.addGrantedLock(resourceID, mode);
                return true;
            }

            int eix = lInfo.findInGrantedList(txId, LockInfo.ANY_LOCK_MODE);
            if (eix == LockInfo.NOT_FOUND) { // First lock by this Txr on the
                                             // resource
                if (!lInfo.isConvertListEmpty()) { // If Some converter(s)
                                                   // is(are) waiting, Txr needs
                                                   // to wait for fairness

                    // ----Deadlock Detection ---
                    if (!isDeadlockFree(txId, resourceID)) {
                        if (LOGGER.isLoggable(Level.INFO)) {
                            LOGGER.info("DEADLOCK DETECTED FOR TRANSACTION " + txId);
                        }
                        context.setStatus(TransactionContext.TIMED_OUT_SATUS);
                        shouldAbort = true;
                    }
                    // ---------------------------

                    else { // Safe to wait
                        waitObj = woManager.allocate();
                        if (waitObj == null) {
                            throw new ACIDException("Invalid (null) object allocated as the WaitEntry for Txr " + txId);
                        }
                        lInfo.addToWaiters(txId, mode, waitObj);
                        txrInfo.setWaitOnRid(resourceID);
                        context.setStartWaitTime(System.currentTimeMillis());

                    }
                } else { // No converter(s) waiting
                    int mask = lInfo.getMask();
                    if (lMtx.conflicts(mask, mode)) { // If There is conflict,
                                                      // Txr needs to wait

                        // ----Deadlock Detection ---
                        if (!isDeadlockFree(txId, resourceID)) {
                            if (LOGGER.isLoggable(Level.INFO)) {
                                LOGGER.info("DEADLOCK DETECTED FOR TRANSACTION " + txId);
                            }
                            context.setStatus(TransactionContext.TIMED_OUT_SATUS);
                            shouldAbort = true;
                        }
                        // ---------------------------
                        else { // Safe to wait
                            waitObj = woManager.allocate();
                            if (waitObj == null) {
                                throw new ACIDException("Invalid (null) object allocated as the WaitEntry for Txr "
                                        + txId);
                            }
                            lInfo.addToWaiters(txId, mode, waitObj);
                            txrInfo.setWaitOnRid(resourceID);
                            context.setStartWaitTime(System.currentTimeMillis());
                        }
                    } else { // No conflicts with the current mask, just grant
                        // it
                        lInfo.addToGranted(txId, mode, LockInfo.NOT_FOUND);
                        txrInfo.addGrantedLock(resourceID, mode);
                        return true;
                    }
                }
            }

            else { // Redundant or Conversion
                grantedMode = lInfo.getGrantedLockMode(txId, eix);
                if (grantedMode == mode) {
                    lInfo.grantRedundantLock(mode, eix);
                    return true; // No need to update tInfo
                } else {
                    if (lMtx.isConversion(grantedMode, mode)) {
                        isConverting = true;
                    } else {
                        return true; // Txr already has a stronger lock on the
                                     // resource
                    }

                }
            }
        }

        if (isConverting) {
            try {
                return convertLockForNewTransaction(context, resourceID, grantedMode, mode);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        if (shouldAbort) {
            requestTxrAbort(context);
            return false;
        }

        // Txr needs to wait and it is safe to wait
        synchronized (waitObj) {
            while (waitObj.needWait()) {
                try {
                    waitObj.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            // Txr just woke up
            woManager.deAllocate(waitObj);
            if (context.getStatus() == TransactionContext.TIMED_OUT_SATUS) { // selected
                                                                             // as
                                                                             // a
                                                                             // victim
                requestTxrAbort(context);
                return false;
            }
        }

        synchronized (context) {
            context.setStatus(TransactionContext.ACTIVE_STATUS);
            context.setStartWaitTime(TransactionContext.INVALID_TIME);
        }

        synchronized (lmTables) {
            txrInfo = lmTables.getTxrInfo(txId);
            if (txrInfo == null) {
                throw new ACIDException("Transaction " + txId + " removed from Txr Table Unexpectedlly");
            }
            txrInfo.addGrantedLock(resourceID, mode);
            txrInfo.setWaitOnRid(null);
        }

        return true; // Arriving here when Txr wakes up and it successfully
                     // locks the resource
    }

    @Override
    public boolean convertLock(TransactionContext context, byte[] resourceID, int mode) throws ACIDException {
        long txId = context.getTransactionID();
        int curMode = -1;
        TxrInfo txrInfo = null;
        LockInfo lInfo = null;
        synchronized (lmTables) {
            txrInfo = lmTables.getTxrInfo(txId);

            if (txrInfo == null) {
                throw new ACIDException("No lock is granted to the transaction, to convert");
            }

            TInfo tInfo = txrInfo.getTxrInfo(resourceID, LockInfo.ANY_LOCK_MODE, TxrInfo.NOT_KNOWN_IX);
            if (tInfo == null) {
                throw new ACIDException("No lock is granted to the transaction on the resource, to convert");
            }

            curMode = tInfo.getMode();
            if (mode == curMode) { // Redundant
                return true; // We do not increment the counter, because it is a
                // conversion
            }

            if (!lMtx.isConversion(curMode, mode)) {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Transaction " + txId + " already has grabbed a stronger mode (" + curMode + ") than "
                            + mode);
                }

                return true;
            }

            lInfo = lmTables.getLockInfo(resourceID);
            if (lInfo == null) {
                throw new ACIDException("No lock on the resource, to convert");
            }
        }

        try {
            return convertLockForNewTransaction(context, resourceID, curMode, mode);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        throw new ACIDException("Problem in Lock Converting for Transaction " + txId
                + " (We unexpectedly returned from convert lock for new transaction)");
    }

    private boolean convertLockForNewTransaction(TransactionContext context, byte[] resourceId, int curMode, int reqMode)
            throws ACIDException, InterruptedException {
        long txId = context.getTransactionID();
        WaitEntry waitObj = null;
        boolean shouldAbort = false;
        TxrInfo txrInfo = null;
        synchronized (lmTables) {
            LockInfo lInfo = lmTables.getLockInfo(resourceId);
            txrInfo = lmTables.getTxrInfo(txId);
            // ---Check if the conversion is already done---
            int eix = lInfo.findInGrantedList(txId, reqMode);
            if (eix != LockInfo.NOT_FOUND) {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Conversion already done for Transaction " + txId + " for lock " + reqMode
                            + " on resource ");
                }
                return true;
            }
            // --------------------------------------------

            int updatedMask = lInfo.getUpdatedMask(txId, curMode, LockInfo.UNKNOWN_IX);
            if (lMtx.conflicts(updatedMask, reqMode)) { // if Conflicting, Txr
                                                        // needs to wait

                // ---- Deadlock Detection ---
                if (!isDeadlockFree(txId, resourceId)) {
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("DEADLOCK DETECTED IN CONVERSION FOR TRANSACTION ");
                    }
                    context.setStatus(TransactionContext.TIMED_OUT_SATUS);
                    shouldAbort = true;
                }
                // ---------------------------

                else {
                    waitObj = woManager.allocate();
                    if (waitObj == null) {
                        throw new ACIDException("Invalid (null) object allocated as the WaitEntry for Txr " + txId);
                    }
                    lInfo.addToConvert(txId, reqMode, waitObj);
                    txrInfo.setWaitOnRid(resourceId);
                    context.setStartWaitTime(System.currentTimeMillis());
                }
            }

            else { // no conflicts, grant it
                lInfo.removeFromGranted(txId, curMode, LockInfo.UNKNOWN_IX);
                lInfo.addToGranted(txId, reqMode, LockInfo.NOT_FOUND);
                txrInfo.removeLock(resourceId, curMode, TxrInfo.NOT_KNOWN_IX);
                txrInfo.addGrantedLock(resourceId, reqMode);
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Transaction " + txId + " could convert to " + reqMode + " lock on resource ");
                }
                return true;
            }
        }

        if (shouldAbort) {
            requestTxrAbort(context);
            return false;
        }

        // Txr needs to wait, and it is safe
        synchronized (waitObj) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Transaction " + txId + " needs to wait for convert " + reqMode);
            }
            while (waitObj.needWait()) {

                waitObj.wait();
            }

            if (context.getStatus() == TransactionContext.TIMED_OUT_SATUS) { // selected
                // as
                // a
                // victim
                requestTxrAbort(context);
                woManager.deAllocate(waitObj);
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Transaction " + txId + " wakes up and victimied for convert " + reqMode);
                }
                return false;
            }
        }

        synchronized (context) {
            context.setStatus(TransactionContext.ACTIVE_STATUS);
            context.setStartWaitTime(TransactionContext.INVALID_TIME);
        }

        synchronized (lmTables) {
            txrInfo = lmTables.getTxrInfo(txId);
            if (txrInfo == null) {
                throw new ACIDException("Transaction " + txId + " removed from Txr Table Unexpectedlly");
            }
            txrInfo.removeLock(resourceId, curMode, TxrInfo.NOT_KNOWN_IX);
            txrInfo.addGrantedLock(resourceId, reqMode);
            txrInfo.setWaitOnRid(null);
        }

        woManager.deAllocate(waitObj);
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Transaction " + txId + " wakes up and convert to " + reqMode);
        }
        return true;
    }

    @Override
    public boolean unlock(TransactionContext context, byte[] resourceID) throws ACIDException {
        long txId = context.getTransactionID();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Transaction " + txId + " wants to unlock on ");
        }
        synchronized (lmTables) {
            TxrInfo txrInfo = lmTables.getTxrInfo(txId);
            if (txrInfo == null) {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Transaction " + txId + " has no locks on resource ");
                }
                return true;
            }

            TInfo transactionInfo = txrInfo.getTxrInfo(resourceID, LockInfo.ANY_LOCK_MODE, TxrInfo.NOT_KNOWN_IX);
            if (transactionInfo == null) {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Transaction " + txId + " has no locks on resource ");
                }
                return true;
            }

            int lockMode = transactionInfo.getMode();

            LockInfo lInfo = (LockInfo) lmTables.getLockInfo(resourceID);
            if (lInfo == null) {
                throw new ACIDException("Trying to unlock() a lock, on a non-existing resource");
            }
            txrInfo.removeLock(resourceID, lockMode, TxrInfo.NOT_KNOWN_IX);
            lInfo.removeFromGranted(txId, lockMode, LockInfo.UNKNOWN_IX);

            Iterator<WaitingInfo> convIt = lInfo.getIteratorOnConverter();
            while (convIt.hasNext()) {
                WaitingInfo nextConvInfo = convIt.next();
                if (nextConvInfo.isVictim()) {
                    continue;
                }
                WaitEntry nextConv = nextConvInfo.getWaitingEntry();
                synchronized (nextConv) {
                    int reqIx = nextConv.getIX(); // entry ix for the (new)
                    // requested lock
                    long convIx = lInfo.getTxId(reqIx);
                    long convTxId = lInfo.getTxId(reqIx);
                    int curConvMode = lInfo.getGrantedLockMode(convTxId, LockInfo.UNKNOWN_IX);
                    int reqConvMode = lInfo.getLockMode(reqIx);
                    int updatedMask = lInfo.getUpdatedMask(convIx, curConvMode, LockInfo.UNKNOWN_IX);
                    if (lMtx.conflicts(updatedMask, reqConvMode)) { // We found
                                                                    // conflict,
                                                                    // no more
                                                                    // transactions
                                                                    // need to
                                                                    // be waken
                                                                    // up
                        context.setStartWaitTime(TransactionContext.INVALID_TIME);
                        if (txrInfo.getSize() == 0) {
                            lmTables.removeTxrInfo(txId);
                            if (LOGGER.isLoggable(Level.INFO)) {
                                LOGGER.info("Entry for Transaction " + txId + " removed from Txr Table (in unlock)");
                            }
                        }
                        if (LOGGER.isLoggable(Level.INFO)) {
                            LOGGER.info("Transaction " + txId + " unlocked its lock");
                        }
                        return true;
                    }
                    // Converter is ready to wake up
                    lmTables.getTxrInfo(convTxId).getContext().setStatus(TransactionContext.ACTIVE_STATUS);
                    lInfo.removeFromGranted(convTxId, curConvMode, LockInfo.UNKNOWN_IX /* curIx */);
                    lInfo.addToGranted(convTxId, reqConvMode, LockInfo.NOT_FOUND);
                    lInfo.prepareToRemoveFromConverters(convTxId, reqConvMode, reqIx);
                    nextConv.wakeUp();
                    convIt.remove();
                    nextConv.notifyAll();
                }
            }

            Iterator<WaitingInfo> waitIt = lInfo.getIteratorOnWaiters();
            while (waitIt.hasNext()) {
                WaitingInfo nextWaiterInfo = waitIt.next();
                if (nextWaiterInfo.isVictim()) {
                    continue;
                }
                WaitEntry nextWaiter = nextWaiterInfo.getWaitingEntry();
                synchronized (nextWaiter) {
                    int waitIx = nextWaiter.getIX();
                    long waitTxId = lInfo.getTxId(waitIx);
                    int reqLock = lInfo.getLockMode(waitIx);
                    int mask = lInfo.getMask();
                    if (lMtx.conflicts(mask, reqLock)) {
                        if (LOGGER.isLoggable(Level.INFO)) {
                            LOGGER.info("Transaction " + txId + " unlocked its lock on ");
                        }

                        context.setStartWaitTime(TransactionContext.INVALID_TIME);
                        if (txrInfo.getSize() == 0) {
                            lmTables.removeTxrInfo(txId);
                            if (LOGGER.isLoggable(Level.INFO)) {
                                LOGGER.info("Entry for Transaction " + txId + " removed from Txr Table (in unlock)");
                            }
                        }
                        return true;
                    }
                    lInfo.addToGranted(waitTxId, reqLock, LockInfo.NOT_FOUND);
                    lInfo.prepareToRemoveFromWaiters(waitTxId, reqLock, waitIx);
                    nextWaiter.wakeUp();
                    waitIt.remove();
                    nextWaiter.notifyAll();
                }
            }
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Transaction " + txId + " unlocked its lock");
            }
            context.setStartWaitTime(TransactionContext.INVALID_TIME);
            if (txrInfo.getSize() == 0) {
                lmTables.removeTxrInfo(txId);
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Entry for Transaction " + txId + " removed from Txr Table (in unlock)");
                }
            }
            return true;
        }
    }

    @Override
    public boolean getInstantlock(TransactionContext context, byte[] resourceID, int mode) throws ACIDException {
        throw new ACIDException("Instant Locking is not supported");
    }

    public Iterator<Long> getTxrInfoIterator() {
        return lmTables.getIteratorOnTxrs();
    }

    @Override
    public synchronized Boolean releaseLocks(TransactionContext context) throws ACIDException {
        long txId = context.getTransactionID();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Entry for Transaction " + txId + " removed from Txr Table (in unlock)");
        }
        synchronized (lmTables) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Transaction " + txId + " started releasing its locks !");
            }
            TxrInfo txrInfo = lmTables.getTxrInfo(txId);
            if (txrInfo == null) {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Transaction with ID " + txId
                            + " has no locks to release. (Returning from Release Locks)");
                }
                return true;
            }
            // First Remove from the waiting list (if waiting)
            byte[] waitOnRid = txrInfo.getWaitOnRid();
            if (waitOnRid != null) {
                LockInfo lInfo = (LockInfo) lmTables.getLockInfo(waitOnRid);
                if ((lInfo.removeFromConverters(txId)) == null) {
                    if ((lInfo.removeFromWaiters(txId)) == null) {
                        throw new ACIDException("Transaction " + txId
                                + " Not Found in the convert/wait list of the resource, it should have waited for");
                    }
                }
            }

            Iterator<TInfo> tInfoIt = txrInfo.getIterator();

            while (tInfoIt.hasNext()) {
                TInfo nextInfo = tInfoIt.next();
                byte[] nextRid = nextInfo.getResourceId();
                int nextLockMode = nextInfo.getMode();
                LockInfo lInfo = lmTables.getLockInfo(nextRid);
                lInfo.removeFromGranted(txId, nextLockMode, LockInfo.UNKNOWN_IX); // Remove
                                                                                  // transaction's
                                                                                  // granted
                                                                                  // lock
                // Now lets try to wake up Waiting Transactions
                // First go through the ConvertList
                Iterator<WaitingInfo> convIt = lInfo.getIteratorOnConverter();
                boolean checkWaiters = true;
                while (convIt.hasNext()) {
                    WaitingInfo nextConvInfo = convIt.next();
                    if (nextConvInfo.isVictim()) {
                        continue;
                    }
                    WaitEntry nextConv = nextConvInfo.getWaitingEntry();
                    synchronized (nextConv) {
                        int reqIx = nextConv.getIX();
                        long convIx = lInfo.getTxId(reqIx);
                        int curIx = lInfo.findInGrantedList(convIx, LockInfo.ANY_LOCK_MODE); // index
                                                                                             // of
                                                                                             // the
                                                                                             // entry
                                                                                             // for
                                                                                             // the
                                                                                             // (old)
                                                                                             // already
                                                                                             // granted
                                                                                             // lock
                        long convTxId = lInfo.getTxId(reqIx);
                        int curConvMode = lInfo.getGrantedLockMode(convTxId, curIx);
                        int reqConvMode = lInfo.getLockMode(reqIx);
                        int updatedMask = lInfo.getUpdatedMask(convIx, curConvMode, curIx);
                        if (lMtx.conflicts(updatedMask, reqConvMode)) {
                            checkWaiters = false;
                            break;
                        }
                        lInfo.removeFromGranted(convTxId, curConvMode, curIx);
                        lInfo.addToGranted(convTxId, reqConvMode, LockInfo.NOT_FOUND);
                        lInfo.prepareToRemoveFromConverters(convTxId, reqConvMode, reqIx);
                        lmTables.getTxrInfo(convTxId).getContext().setStartWaitTime(TransactionContext.INVALID_TIME);
                        nextConv.wakeUp();
                        convIt.remove();
                        nextConv.notifyAll();
                    }
                }

                if (checkWaiters) {
                    // Going through the WaitList
                    Iterator<WaitingInfo> waitIt = lInfo.getIteratorOnWaiters();
                    while (waitIt.hasNext()) {
                        WaitingInfo nextWaiterInfo = waitIt.next();
                        if (nextWaiterInfo.isVictim()) {
                            continue;
                        }
                        WaitEntry nextWaiter = nextWaiterInfo.getWaitingEntry();
                        synchronized (nextWaiter) {
                            int waitIx = nextWaiter.getIX();
                            long waitTxId = lInfo.getTxId(waitIx);
                            int reqLock = lInfo.getLockMode(waitIx);
                            int mask = lInfo.getMask();
                            if (lMtx.conflicts(mask, reqLock)) {
                                break;
                            }
                            lInfo.addToGranted(waitTxId, reqLock, LockInfo.NOT_FOUND);
                            lInfo.prepareToRemoveFromWaiters(waitTxId, reqLock, waitIx);
                            lmTables.getTxrInfo(waitTxId).getContext()
                                    .setStartWaitTime(TransactionContext.INVALID_TIME);
                            nextWaiter.wakeUp();
                            waitIt.remove();
                            nextWaiter.notifyAll();
                        }
                    }
                }
            }

            context.setStartWaitTime(TransactionContext.INVALID_TIME);
            if ((lmTables.removeTxrInfo(txId)) == null) { // Remove Txr's entry
                                                          // from the
                                                          // transactions' table
                throw new ACIDException("Transaction " + txId + " Not found in transactions table for removal");
            } else {
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Entry for Transaction " + txId + " removed from Txr Table (in release locks)");
                }
            }
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("Transaction " + txId + " released its locks successfully !");
            }

            return true;
        }
    }

    private boolean isDeadlockFree(long txId, byte[] resourceId) {
        return deadlockDetector.isSafeToAdd(txId, resourceId);
    }

    private void requestTxrAbort(TransactionContext context) throws ACIDException {
        context.setStartWaitTime(TransactionContext.INVALID_TIME);
        throw new ACIDException("Transaction " + context.getTransactionID()
                + " should abort (requested by the Lock Manager)");
    }

    @Override
    public String getDebugLockStatus() throws ACIDException {
        String s = "\nLock Status (For Debug Purpose):\n";
        synchronized (lmTables) {
            Iterator<Long> txIdIt = getTxrInfoIterator();
            while (txIdIt.hasNext()) {
                long nextTxId = txIdIt.next();
                TxrInfo nextInfoList = lmTables.getTxrInfo(nextTxId);
                byte[] nextWaitOnRid = nextInfoList.getWaitOnRid();
                String status = (nextWaitOnRid == null ? " ACTIVE" : " WAITING");
                if ((nextWaitOnRid != null)) {
                    LockInfo lInfo = (LockInfo) lmTables.getLockInfo(nextWaitOnRid);
                    int wlModeIx = lInfo.findInConvertList(nextTxId, LockInfo.ANY_LOCK_MODE);
                    if (wlModeIx == LockInfo.NOT_FOUND) {
                        wlModeIx = lInfo.findInWaitList(nextTxId, LockInfo.ANY_LOCK_MODE);
                    }
                    int wlMode = lInfo.getLockMode(wlModeIx);
                    String wLModeRep = (wlMode == 0 ? "S" : "X");
                    status += " for " + wLModeRep + " lock";
                }

                String lockModes = "";
                Iterator<TInfo> tInfoIt = nextInfoList.getIterator();
                while (tInfoIt.hasNext()) {
                    TInfo next = tInfoIt.next();
                    int nextLockMode = next.getMode();
                    lockModes += (nextLockMode == 0 ? "S" : "X");
                    lockModes += ", ";
                }
                s += "Transaction: " + nextTxId + "\t- (Status: " + status + ") --> Granted Locks List: ( " + lockModes
                        + " )\n";
            }

        }
        return s + "\n";
    }

    public void sweepForTimeout() throws ACIDException {
        synchronized (lmTables) {
            Iterator<Long> txrIt = lmTables.getIteratorOnTxrs();
            while (txrIt.hasNext()) {
                long nextTxrID = txrIt.next();
                TxrInfo nextTxrInfo = lmTables.getTxrInfo(nextTxrID);
                if (toutDetector.isVictim(nextTxrInfo)) {
                    nextTxrInfo.getContext().setStatus(TransactionContext.TIMED_OUT_SATUS);
                    LockInfo nextLockInfo = lmTables.getLockInfo(nextTxrInfo.getWaitOnRid());
                    synchronized (nextLockInfo) {
                        WaitingInfo nextVictim = nextLockInfo.getWaitingOnObject(nextTxrID, LockInfo.ANY_LOCK_MODE);
                        nextVictim.setAsVictim();
                        toutDetector.addToVictimsList(nextVictim.getWaitingEntry());
                    }
                }
            }
        }
    }

    public LockInfo getLockInfo(byte[] resourceID) {
        return lmTables.getLockInfo(resourceID);
    }

    public TxrInfo getTxrInfo(long txrId) {
        return lmTables.getTxrInfo(txrId);
    }
}

class LMTables {
    /**
     * An instance of this class mainly manages and synchronizes the access to
     * the lock manager hash tables
     */

    private ILockHashTable<byte[], LockInfo> resToLInfo; // mapping from
                                                         // resourceID to
                                                         // information about
                                                         // the locks on it
    private ILockHashTable<Long, TxrInfo> txToTxrInfo; // mapping from
                                                       // transactionID to
                                                       // informations about its
                                                       // lock(s)

    public LMTables(int initialSize) {
        resToLInfo = new ResourcesHT(initialSize);
        txToTxrInfo = new TransactionsHT(initialSize);
    }

    public LockInfo getLockInfo(byte[] resourceId) {
        return resToLInfo.get(resourceId);
    }

    public void putLockInfo(byte[] resourceID, LockInfo lInfo) {
        resToLInfo.put(resourceID, lInfo);
    }

    public TxrInfo getTxrInfo(long txrId) {
        return txToTxrInfo.get(txrId);
    }

    public void putTxrInfo(long txrId, TxrInfo txrInfo) {
        txToTxrInfo.put(txrId, txrInfo);
    }

    public TxrInfo removeTxrInfo(long txId) {
        return txToTxrInfo.remove(txId);
    }

    public int getTxrTableSize() {
        return txToTxrInfo.getKeysetSize();
    }

    public Iterator<Long> getIteratorOnTxrs() {
        return ((TransactionsHT) txToTxrInfo).getIteratorOnTxs();
    }

}

class ResourcesHT implements ILockHashTable<byte[], LockInfo> {

    private Hashtable<LockTag, LockInfo> table;
    private LockTag tag;

    public ResourcesHT(int initCapacity) {
        this.table = new Hashtable<LockTag, LockInfo>(initCapacity);
        this.tag = new LockTag(null);
    }

    @Override
    public synchronized void put(byte[] rsId, LockInfo info) {
        table.put(new LockTag(rsId), (LockInfo) info);
    }

    @Override
    public synchronized LockInfo get(byte[] rsId) {
        tag.setRsId(rsId);
        return (table.get(tag));
    }

    @Override
    public LockInfo remove(byte[] rsId) {
        tag.setRsId(rsId);
        return (table.remove(tag));
    }

    @Override
    public int getKeysetSize() {
        return table.size();
    }

}

class TransactionsHT implements ILockHashTable<Long, TxrInfo> {

    private Hashtable<Long, TxrInfo> table;

    public TransactionsHT(int initCapacity) {
        this.table = new Hashtable<Long, TxrInfo>(initCapacity);
    }

    @Override
    public synchronized void put(Long key, TxrInfo value) {
        table.put(key, value);

    }

    @Override
    public synchronized TxrInfo get(Long key) {
        return (table.get(key));
    }

    public Iterator<Long> getIteratorOnTxs() {
        return table.keySet().iterator();
    }

    @Override
    public TxrInfo remove(Long key) {
        return table.remove(key);
    }

    @Override
    public int getKeysetSize() {
        return table.size();
    }

}

class LockTag {
    /**
     * Used as a wrapper around byte[], which is used as the key for the
     * hashtables
     */

    byte[] rsId;

    public LockTag(byte[] rsId) {
        setRsId(rsId);
    }

    public void setRsId(byte[] rsId) {
        this.rsId = rsId;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(rsId);
    }

    @Override
    public boolean equals(Object o) {
        if ((o == null) || !(o instanceof LockTag)) {
            return false;
        }
        return Arrays.equals(((LockTag) o).rsId, this.rsId);
    }
}

class WaitObjectManager {
    /**
     * Manages the set of waiting objects (objects used to manage waiters) to
     * avoid object/garbage creation as much as possible
     */
    final int EOL = -1;
    ArrayList<WaitEntry> list;
    AtomicInteger max;
    int nextFree;

    public WaitObjectManager() {
        list = new ArrayList<WaitEntry>();
        nextFree = EOL;
        max = new AtomicInteger(0);
    }

    public WaitEntry allocate() throws ACIDException {
        WaitEntry o = null;
        synchronized (list) {
            if (nextFree == EOL) {
                o = new WaitEntry(max.getAndIncrement(), LockInfo.UNKNOWN_IX, EOL);
                list.add(o);
                return o;
            }
            o = list.get(nextFree);
            nextFree = o.getNext();
            o.setNext(EOL);
        }
        return o;
    }

    public void deAllocate(Object o) {
        synchronized (list) {
            ((WaitEntry) o).setNext(nextFree);
            nextFree = ((WaitEntry) o).getId();
        }
    }

}

class WaitEntry {
    /**
     * Captures the information about a waiting transaction
     */

    private int id; // ID of this object (used for managing the waiting objects
                    // and recycling them)
    private int eix; // index of the entry corresponding to the waiting
                     // transaction
    private boolean shouldWait; // whether the waiter needs to continue its
                                // waiting or not
    private int next; // The next waitEntry in the chain of wait Entries (used
                      // for managing the waiting objects and recycling them)

    public WaitEntry(int id, int eix, int next) {
        this.id = id;
        this.eix = eix;
        shouldWait = true;
        this.next = next;
    }

    public int getIX() {
        return eix;
    }

    public void setIx(int eix) {
        this.eix = eix;
    }

    public int getId() {
        return id;
    }

    public void setNext(int n) {
        next = n;
    }

    public int getNext() {
        return next;
    }

    public boolean needWait() {
        return shouldWait;
    }

    public void wakeUp() {
        this.shouldWait = false;
    }

    public void setForWait() {
        this.shouldWait = true;
    }
}