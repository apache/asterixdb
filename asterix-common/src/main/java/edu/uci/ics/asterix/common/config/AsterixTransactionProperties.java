package edu.uci.ics.asterix.common.config;

public class AsterixTransactionProperties extends AbstractAsterixProperties {
    private static final String TXN_LOG_DIRECTORY_KEY = "txn.log.directory";
    private static final String TXN_LOG_DIRECTORY_DEFAULT = "asterix_logs/";
    
    private static final String TXN_LOG_BUFFER_NUMPAGES_KEY = "txn.log.buffer.numpages";
    private static int TXN_LOG_BUFFER_NUMPAGES_DEFAULT = 8;

    private static final String TXN_LOG_BUFFER_PAGESIZE_KEY = "txn.log.buffer.pagesize";
    private static final int TXN_LOG_BUFFER_PAGESIZE_DEFAULT = (128 << 10); // 128KB

    private static final String TXN_LOG_PARTITIONSIZE_KEY = "txn.log.partitionsize";
    private static final long TXN_LOG_PARTITIONSIZE_DEFAULT = (2 << 30); // 2GB
    
    private static final String TXN_LOG_DISKSECTORSIZE_KEY = "txn.log.disksectorsize";
    private static final int TXN_LOG_DISKSECTORSIZE_DEFAULT = 4096;

    private static final String TXN_LOG_GROUPCOMMITINTERVAL_KEY = "txn.log.groupcommitinterval";
    private static int TXN_LOG_GROUPCOMMITINTERVAL_DEFAULT = 10; // 0.1ms

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
    
    public String getLogDirectory() {
        String logDirectory = accessor.getProperty(TXN_LOG_DIRECTORY_KEY, TXN_LOG_DIRECTORY_DEFAULT,
                PropertyInterpreters.getStringPropertyInterpreter());
        if (!logDirectory.endsWith("/")) {
            logDirectory += "/";
        }
        return logDirectory;
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
    
    public int getLogDiskSectorSize() {
        return accessor.getProperty(TXN_LOG_DISKSECTORSIZE_KEY, TXN_LOG_DISKSECTORSIZE_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    public int getGroupCommitInterval() {
        return accessor.getProperty(TXN_LOG_GROUPCOMMITINTERVAL_KEY, TXN_LOG_GROUPCOMMITINTERVAL_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
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
