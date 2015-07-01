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
package edu.uci.ics.asterix.transaction.management.service.recovery;

import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.transactions.ILogManager;
import edu.uci.ics.asterix.common.transactions.IRecoveryManager;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManager;

public class CheckpointThread extends Thread {

    private long lsnThreshold;
    private long checkpointTermInSecs;

    private final ILogManager logManager;
    private final IRecoveryManager recoveryMgr;

    public CheckpointThread(IRecoveryManager recoveryMgr, IIndexLifecycleManager indexLifecycleManager, ILogManager logManager,
            long lsnThreshold, long checkpointTermInSecs) {
        this.recoveryMgr = recoveryMgr;
        this.logManager = logManager;
        this.lsnThreshold = lsnThreshold;
        this.checkpointTermInSecs = checkpointTermInSecs;
    }

    @Override
    public void run() {
        
        Thread.currentThread().setName("Checkpoint Thread");

        long currentCheckpointAttemptMinLSN = -1;
        long lastCheckpointLSN = -1;
        long currentLogLSN = 0;
        long targetCheckpointLSN = 0;
        while (true) {
            try {
                sleep(checkpointTermInSecs * 1000);
            } catch (InterruptedException e) {
                //ignore
            }
            
            
            if(lastCheckpointLSN == -1)
            {
                try {
                    //Since the system just started up after sharp checkpoint, last checkpoint LSN is considered as the min LSN of the current log partition
                    lastCheckpointLSN = logManager.getReadableSmallestLSN();
                } catch (Exception e) {
                    lastCheckpointLSN = 0; 
                }
            }
            
            //1. get current log LSN
            currentLogLSN = logManager.getAppendLSN();
            
            //2. if current log LSN - previous checkpoint > threshold, do checkpoint
            if (currentLogLSN - lastCheckpointLSN > lsnThreshold) {
                try {
                    // in check point:
                    //1. get minimum first LSN (MFL) from open indexes.
                    //2. if current MinFirstLSN < targetCheckpointLSN, schedule async flush for any open index witch has first LSN < force flush delta
                    //3. next time checkpoint comes, it will be able to remove log files which have end range less than current targetCheckpointLSN
                   
                    targetCheckpointLSN = lastCheckpointLSN + lsnThreshold;
                    currentCheckpointAttemptMinLSN = recoveryMgr.checkpoint(false, targetCheckpointLSN);
                    
                    //checkpoint was completed at target LSN or above
                    if(currentCheckpointAttemptMinLSN >= targetCheckpointLSN)
                    {
                        lastCheckpointLSN = currentCheckpointAttemptMinLSN; 
                    }
                    
                } catch (ACIDException | HyracksDataException e) {
                    throw new Error("failed to checkpoint", e);
                }
            }
        }
    }

}