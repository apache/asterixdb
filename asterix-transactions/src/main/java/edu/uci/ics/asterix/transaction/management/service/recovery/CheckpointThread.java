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

import java.util.List;

import edu.uci.ics.asterix.common.exceptions.ACIDException;
import edu.uci.ics.asterix.common.ioopcallbacks.AbstractLSMIOOperationCallback;
import edu.uci.ics.asterix.common.transactions.IRecoveryManager;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IIndex;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexLifecycleManager;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;

public class CheckpointThread extends Thread {

    private long lsnThreshold;
    private long checkpointTermInSecs;

    private long lastMinMCTFirstLSN = 0;

    private final IRecoveryManager recoveryMgr;
    private final IIndexLifecycleManager indexLifecycleManager;

    public CheckpointThread(IRecoveryManager recoveryMgr, IIndexLifecycleManager indexLifecycleManager,
            long lsnThreshold, long checkpointTermInSecs) {
        this.recoveryMgr = recoveryMgr;
        this.indexLifecycleManager = indexLifecycleManager;
        this.lsnThreshold = lsnThreshold;
        this.checkpointTermInSecs = checkpointTermInSecs;
    }

    @Override
    public void run() {
        long currentMinMCTFirstLSN = 0;
        while (true) {
            try {
                sleep(checkpointTermInSecs * 1000);
            } catch (InterruptedException e) {
                //ignore
            }

            currentMinMCTFirstLSN = getMinMCTFirstLSN();
            if (currentMinMCTFirstLSN - lastMinMCTFirstLSN > lsnThreshold) {
                try {
                    recoveryMgr.checkpoint(false);
                    lastMinMCTFirstLSN = currentMinMCTFirstLSN;
                } catch (ACIDException | HyracksDataException e) {
                    throw new Error("failed to checkpoint", e);
                }
            }
        }
    }

    private long getMinMCTFirstLSN() {
        List<IIndex> openIndexList = indexLifecycleManager.getOpenIndexes();
        long minMCTFirstLSN = Long.MAX_VALUE;
        long firstLSN;
        if (openIndexList.size() > 0) {
            for (IIndex index : openIndexList) {
                firstLSN = ((AbstractLSMIOOperationCallback) ((ILSMIndex) index).getIOOperationCallback())
                        .getFirstLSN();
                minMCTFirstLSN = Math.min(minMCTFirstLSN, firstLSN);
            }
        } else {
            minMCTFirstLSN = -1;
        }
        return minMCTFirstLSN;
    }
}
