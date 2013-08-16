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
package edu.uci.ics.asterix.common.transactions;

import edu.uci.ics.asterix.common.transactions.ITransactionManager.TransactionState;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;

public interface ITransactionContext {

    public void registerIndexAndCallback(long resourceId, ILSMIndex index, AbstractOperationCallback callback,
            boolean isPrimaryIndex);

    public JobId getJobId();

    public void setStartWaitTime(long time);

    public long getStartWaitTime();

    public void setStatus(int status);

    public int getStatus();

    public void setTxnState(TransactionState txnState);

    public TransactionState getTxnState();

    public long getFirstLSN();

    public long getLastLSN();

    public void setLastLSN(long resourceId, long LSN);

    public TransactionType getTransactionType();

    public void setTransactionType(TransactionType transactionType);

    public String prettyPrint();

    // used for showing a transaction is not waiting.
    public static final long INVALID_TIME = -1l;

    public static final int ACTIVE_STATUS = 0;
    public static final int TIMED_OUT_STATUS = 1;

    public enum TransactionType {
        READ,
        READ_WRITE
    }

    public void setExclusiveJobLevelCommit();

    void notifyOptracker(boolean isJobLevelCommit);

}
