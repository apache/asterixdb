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

import java.util.LinkedList;

import edu.uci.ics.asterix.common.api.AsterixThreadExecutor;
import edu.uci.ics.asterix.common.exceptions.ACIDException;

/**
 * @author pouria, kisskys
 *         Any transaction which has been waiting for a lock for more
 *         than the predefined time-out threshold is considered to be deadlocked
 *         (this can happen in distributed case for example) An instance of this
 *         class triggers scanning (sweeping) lock manager's transactions table
 *         periodically and detects such timed-out transactions
 */

public class TimeOutDetector {

    LockManager lockMgr;
    Thread trigger;
    LinkedList<LockWaiter> victimList;
    int timeoutThreshold;
    int sweepThreshold;

    public TimeOutDetector(LockManager lockMgr) {
        this.victimList = new LinkedList<LockWaiter>();
        this.lockMgr = lockMgr;
        this.trigger = new Thread(new TimeoutTrigger(this));
        this.timeoutThreshold = lockMgr.getTransactionProperties().getTimeoutWaitThreshold();
        this.sweepThreshold = lockMgr.getTransactionProperties().getTimeoutSweepThreshold();
        trigger.setDaemon(true);
        AsterixThreadExecutor.INSTANCE.execute(trigger);
    }

    public void sweep() throws ACIDException {
        victimList.clear();
        // Initiates the time-out sweeping process
        // from the lockManager
        lockMgr.sweepForTimeout();
        notifyVictims();
    }

    public void checkAndSetVictim(LockWaiter waiterObj) {
        if (System.currentTimeMillis() - waiterObj.getBeginWaitTime() >= timeoutThreshold) {
            waiterObj.setVictim(true);
            waiterObj.setWait(false);
            victimList.add(waiterObj);
        }
    }

    private void notifyVictims() {
        for (LockWaiter waiterObj : victimList) {
            synchronized (waiterObj) {
                waiterObj.notifyAll();
            }
        }
        victimList.clear();
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
                Thread.sleep(owner.sweepThreshold);
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
