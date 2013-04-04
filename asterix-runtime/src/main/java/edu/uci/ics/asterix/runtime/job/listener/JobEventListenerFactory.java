package edu.uci.ics.asterix.runtime.job.listener;

import edu.uci.ics.asterix.common.context.AsterixAppRuntimeContext;
import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.service.transaction.DatasetId;
import edu.uci.ics.asterix.transaction.management.service.transaction.ITransactionManager;
import edu.uci.ics.asterix.transaction.management.service.transaction.JobId;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext.TransactionType;
import edu.uci.ics.hyracks.api.context.IHyracksJobletContext;
import edu.uci.ics.hyracks.api.job.IJobletEventListener;
import edu.uci.ics.hyracks.api.job.IJobletEventListenerFactory;
import edu.uci.ics.hyracks.api.job.JobStatus;

public class JobEventListenerFactory implements IJobletEventListenerFactory {

    private static final long serialVersionUID = 1L;
    private final JobId jobId;
    private final boolean transactionalWrite;

    public JobEventListenerFactory(JobId jobId, boolean transactionalWrite) {
        this.jobId = jobId;
        this.transactionalWrite = transactionalWrite;
    }
    
    public JobId getJobId() {
        return jobId;
    }

    @Override
    public IJobletEventListener createListener(final IHyracksJobletContext jobletContext) {

        return new IJobletEventListener() {
            @Override
            public void jobletFinish(JobStatus jobStatus) {
                try {
                    ITransactionManager txnManager = ((AsterixAppRuntimeContext) jobletContext.getApplicationContext()
                            .getApplicationObject()).getTransactionSubsystem().getTransactionManager();
                    TransactionContext txnContext = txnManager.getTransactionContext(jobId);
                    txnContext.setTransactionType(transactionalWrite ? TransactionType.READ_WRITE
                            : TransactionType.READ);
                    txnManager.completedTransaction(txnContext, new DatasetId(-1), -1, !(jobStatus == JobStatus.FAILURE));
                } catch (ACIDException e) {
                    throw new Error(e);
                }
            }

            @Override
            public void jobletStart() {
                try {
                    ((AsterixAppRuntimeContext) jobletContext.getApplicationContext().getApplicationObject())
                            .getTransactionSubsystem().getTransactionManager().getTransactionContext(jobId);
                } catch (ACIDException e) {
                    throw new Error(e);
                }
            }

        };
    }
}
