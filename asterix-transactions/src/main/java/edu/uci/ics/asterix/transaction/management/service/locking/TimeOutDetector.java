package edu.uci.ics.asterix.transaction.management.service.locking;

import java.util.LinkedList;

import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext;

/**
 * @author pouria Any transaction which has been waiting for a lock for more
 *         than the predefined time-out threshold is considered to be deadlocked
 *         (this can happen in distributed case for example) An instance of this
 *         class triggers scanning (sweeping) lock manager's transactions table
 *         periodically and detects such timed-out transactions
 */

public class TimeOutDetector {
    static final long TIME_OUT_THRESHOLD = 60000;
    static final long SWEEP_PERIOD = 120000;

    LockManager lockMgr;
    Thread trigger;
    LinkedList<WaitEntry> victimsWObjs;

    public TimeOutDetector(LockManager lockMgr) {
        this.victimsWObjs = new LinkedList<WaitEntry>();
        this.lockMgr = lockMgr;
        this.trigger = new Thread(new TimeoutTrigger(this));
        trigger.setDaemon(true);
        trigger.start();
    }

    public void sweep() throws ACIDException {
        victimsWObjs.clear();
        lockMgr.sweepForTimeout(); // Initiates the time-out sweeping process
                                   // from the lockManager
        notifyVictims();
    }

    public boolean isVictim(TxrInfo txrInfo) {
        long sWTime = txrInfo.getStartWaitTime();
        int status = txrInfo.getContext().getStatus();
        return (status != TransactionContext.TIMED_OUT_SATUS && sWTime != TransactionContext.INVALID_TIME && (System
                .currentTimeMillis() - sWTime) >= TIME_OUT_THRESHOLD);
    }

    public void addToVictimsList(WaitEntry wEntry) {
        victimsWObjs.add(wEntry);
    }

    private void notifyVictims() {
        for (WaitEntry w : victimsWObjs) {
            synchronized (w) {
                w.wakeUp();
                w.notifyAll();
            }
        }
        victimsWObjs.clear();
    }

}

class TimeoutTrigger implements Runnable {

    TimeOutDetector owner;

    public TimeoutTrigger(TimeOutDetector owner) {
        this.owner = owner;
    }

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(TimeOutDetector.SWEEP_PERIOD);
                owner.sweep(); // Trigger the timeout detector (the owner) to
                               // initiate sweep
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ACIDException e) {
                throw new IllegalStateException(e);
            }

        }

    }

}
