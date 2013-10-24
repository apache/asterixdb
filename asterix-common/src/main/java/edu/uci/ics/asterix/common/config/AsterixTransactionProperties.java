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
package edu.uci.ics.asterix.common.config;

public class AsterixTransactionProperties extends AbstractAsterixProperties {

    private static final String TXN_LOG_BUFFER_NUMPAGES_KEY = "txn.log.buffer.numpages";
    private static int TXN_LOG_BUFFER_NUMPAGES_DEFAULT = 8;

    private static final String TXN_LOG_BUFFER_PAGESIZE_KEY = "txn.log.buffer.pagesize";
    private static final int TXN_LOG_BUFFER_PAGESIZE_DEFAULT = (128 << 10); // 128KB

    private static final String TXN_LOG_PARTITIONSIZE_KEY = "txn.log.partitionsize";
    private static final long TXN_LOG_PARTITIONSIZE_DEFAULT = ((long)2 << 30); // 2GB

    private static final String TXN_LOG_CHECKPOINT_LSNTHRESHOLD_KEY = "txn.log.checkpoint.lsnthreshold";
    private static final int TXN_LOG_CHECKPOINT_LSNTHRESHOLD_DEFAULT = (64 << 20); // 64M

    private static final String TXN_LOG_CHECKPOINT_POLLFREQUENCY_KEY = "txn.log.checkpoint.pollfrequency";
    private static int TXN_LOG_CHECKPOINT_POLLFREQUENCY_DEFAULT = 120; // 120s

    private static final String TXN_LOG_CHECKPOINT_HISTORY_KEY = "txn.log.checkpoint.history";
    private static int TXN_LOG_CHECKPOINT_HISTORY_DEFAULT = 0;

    private static final String TXN_LOCK_ESCALATIONTHRESHOLD_KEY = "txn.lock.escalationthreshold";
    private static int TXN_LOCK_ESCALATIONTHRESHOLD_DEFAULT = 1000;

    private static final String TXN_LOCK_SHRINKTIMER_KEY = "txn.lock.shrinktimer";
    private static int TXN_LOCK_SHRINKTIMER_DEFAULT = 5000; // 5s

    private static final String TXN_LOCK_TIMEOUT_WAITTHRESHOLD_KEY = "txn.lock.timeout.waitthreshold";
    private static final int TXN_LOCK_TIMEOUT_WAITTHRESHOLD_DEFAULT = 60000; // 60s

    private static final String TXN_LOCK_TIMEOUT_SWEEPTHRESHOLD_KEY = "txn.lock.timeout.sweepthreshold";
    private static final int TXN_LOCK_TIMEOUT_SWEEPTHRESHOLD_DEFAULT = 10000; // 10s

    public AsterixTransactionProperties(AsterixPropertiesAccessor accessor) {
        super(accessor);
    }

    public String getLogDirectory(String nodeId) {
        return accessor.getTransactionLogDir(nodeId);
    }

    public int getLogBufferNumPages() {
        return accessor.getProperty(TXN_LOG_BUFFER_NUMPAGES_KEY, TXN_LOG_BUFFER_NUMPAGES_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    public int getLogBufferPageSize() {
        return accessor.getProperty(TXN_LOG_BUFFER_PAGESIZE_KEY, TXN_LOG_BUFFER_PAGESIZE_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    public long getLogPartitionSize() {
        return accessor.getProperty(TXN_LOG_PARTITIONSIZE_KEY, TXN_LOG_PARTITIONSIZE_DEFAULT,
                PropertyInterpreters.getLongPropertyInterpreter());
    }

    public int getCheckpointLSNThreshold() {
        return accessor.getProperty(TXN_LOG_CHECKPOINT_LSNTHRESHOLD_KEY, TXN_LOG_CHECKPOINT_LSNTHRESHOLD_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    public int getCheckpointPollFrequency() {
        return accessor.getProperty(TXN_LOG_CHECKPOINT_POLLFREQUENCY_KEY, TXN_LOG_CHECKPOINT_POLLFREQUENCY_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    public int getCheckpointHistory() {
        return accessor.getProperty(TXN_LOG_CHECKPOINT_HISTORY_KEY, TXN_LOG_CHECKPOINT_HISTORY_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    public int getEntityToDatasetLockEscalationThreshold() {
        return accessor.getProperty(TXN_LOCK_ESCALATIONTHRESHOLD_KEY, TXN_LOCK_ESCALATIONTHRESHOLD_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    public int getLockManagerShrinkTimer() {
        return accessor.getProperty(TXN_LOCK_SHRINKTIMER_KEY, TXN_LOCK_SHRINKTIMER_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    public int getTimeoutWaitThreshold() {
        return accessor.getProperty(TXN_LOCK_TIMEOUT_WAITTHRESHOLD_KEY, TXN_LOCK_TIMEOUT_WAITTHRESHOLD_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    public int getTimeoutSweepThreshold() {
        return accessor.getProperty(TXN_LOCK_TIMEOUT_SWEEPTHRESHOLD_KEY, TXN_LOCK_TIMEOUT_SWEEPTHRESHOLD_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }

}
