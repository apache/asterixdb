package edu.uci.ics.asterix.common.config;

public class AsterixTransactionProperties extends AbstractAsterixProperties {
    private static final String TXN_LOG_BUFFER_NUMPAGES_KEY = "txn.log.buffer.numpages";
    private static int TXN_LOG_BUFFER_NUMPAGES_DEFAULT = 8;

    private static final String TXN_LOG_BUFFER_PAGESIZE_KEY = "txn.log.buffer.pagesize";
    private static final int TXN_LOG_BUFFER_PAGESIZE_DEFAULT = (128 << 10); // 128KB

    private static final String TXN_LOG_PARTITIONSIZE_KEY = "txn.log.partitionsize";
    private static final int TXN_LOG_PARTITIONSIZE_DEFAULT = (2 << 30); // 2GB

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
        return accessor.getInt(TXN_LOG_BUFFER_NUMPAGES_KEY, TXN_LOG_BUFFER_NUMPAGES_DEFAULT);
    }

    public int getLogBufferPageSize() {
        return accessor.getInt(TXN_LOG_BUFFER_PAGESIZE_KEY, TXN_LOG_BUFFER_PAGESIZE_DEFAULT);
    }

    public int getLogPartitionSize() {
        return accessor.getInt(TXN_LOG_PARTITIONSIZE_KEY, TXN_LOG_PARTITIONSIZE_DEFAULT);
    }

    public int getGroupCommitInterval() {
        return accessor.getInt(TXN_LOG_GROUPCOMMITINTERVAL_KEY, TXN_LOG_GROUPCOMMITINTERVAL_DEFAULT);
    }

    public int getCheckpointLSNThreshold() {
        return accessor.getInt(TXN_LOG_CHECKPOINT_LSNTHRESHOLD_KEY, TXN_LOG_CHECKPOINT_LSNTHRESHOLD_DEFAULT);
    }

    public int getCheckpointPollFrequency() {
        return accessor.getInt(TXN_LOG_CHECKPOINT_POLLFREQUENCY_KEY, TXN_LOG_CHECKPOINT_POLLFREQUENCY_DEFAULT);
    }

    public int getEntityToDatasetLockEscalationThreshold() {
        return accessor.getInt(TXN_LOCK_ESCALATIONTHRESHOLD_KEY, TXN_LOCK_ESCALATIONTHRESHOLD_DEFAULT);
    }

    public int getLockManagerShrinkTimer() {
        return accessor.getInt(TXN_LOCK_SHRINKTIMER_KEY, TXN_LOCK_SHRINKTIMER_DEFAULT);
    }

}
