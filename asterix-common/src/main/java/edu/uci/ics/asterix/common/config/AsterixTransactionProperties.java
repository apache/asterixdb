package edu.uci.ics.asterix.common.config;

public class AsterixTransactionProperties extends AbstractAsterixProperties {
    private static final String TXN_LOG_BUFFER_NUMPAGES_KEY = "txn.log.buffer.numpages";
    private static int TXN_LOG_BUFFER_NUMPAGES_DEFAULT = 8;

    private static final String TXN_LOG_BUFFER_PAGESIZE_KEY = "txn.log.buffer.pagesize";
    private static final int TXN_LOG_BUFFER_PAGESIZE_DEFAULT = (128 << 10); // 128KB

    private static final String TXN_LOG_PARTITIONSIZE_KEY = "txn.log.partitionsize";
    private static final long TXN_LOG_PARTITIONSIZE_DEFAULT = (2 << 30); // 2GB

    private static final String TXN_LOG_GROUPCOMMITINTERVAL_KEY = "txn.log.groupcommitinterval";
    private static int TXN_LOG_GROUPCOMMITINTERVAL_DEFAULT = 200; // 200ms

    private static final String TXN_LOG_CHECKPOINT_LSNTHRESHOLD_KEY = "txn.log.checkpoint.lsnthreshold";
    private static final int TXN_LOG_CHECKPOINT_LSNTHRESHOLD_DEFAULT = (64 << 20); // 64M

    private static final String TXN_LOG_CHECKPOINT_POLLFREQUENCY_KEY = "txn.log.checkpoint.pollfrequency";
    private static int TXN_LOG_CHECKPOINT_POLLFREQUENCY_DEFAULT = 120; // 120s

    private static final String TXN_LOCK_ESCALATIONTHRESHOLD_KEY = "txn.lock.escalationthreshold";
    private static int TXN_LOCK_ESCALATIONTHRESHOLD_DEFAULT = 1000;

    private static final String TXN_LOCK_SHRINKTIMER_KEY = "txn.lock.shrinktimer";
    private static int TXN_LOCK_SHRINKTIMER_DEFAULT = 120000; // 2m

    public AsterixTransactionProperties(AsterixPropertiesAccessor accessor) {
        super(accessor);
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

    public int getEntityToDatasetLockEscalationThreshold() {
        return accessor.getProperty(TXN_LOCK_ESCALATIONTHRESHOLD_KEY, TXN_LOCK_ESCALATIONTHRESHOLD_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }

    public int getLockManagerShrinkTimer() {
        return accessor.getProperty(TXN_LOCK_SHRINKTIMER_KEY, TXN_LOCK_SHRINKTIMER_DEFAULT,
                PropertyInterpreters.getIntegerPropertyInterpreter());
    }

}
