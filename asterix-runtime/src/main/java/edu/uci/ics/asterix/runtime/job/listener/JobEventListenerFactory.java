package edu.uci.ics.asterix.runtime.job.listener;

import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.service.transaction.ITransactionManager;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionProvider;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext.TransactionType;
import edu.uci.ics.hyracks.api.context.IHyracksJobletContext;
import edu.uci.ics.hyracks.api.job.IJobletEventListener;
import edu.uci.ics.hyracks.api.job.IJobletEventListenerFactory;
import edu.uci.ics.hyracks.api.job.JobStatus;

public class JobEventListenerFactory implements IJobletEventListenerFactory {

    private static final long serialVersionUID = 1L;
    private final long txnId;
    private final boolean transactionalWrite;

    public JobEventListenerFactory(long txnId, boolean transactionalWrite) {
        this.txnId = txnId;
        this.transactionalWrite = transactionalWrite;
    }

    @Override
    public IJobletEventListener createListener(final IHyracksJobletContext jobletContext) {

        return new IJobletEventListener() {
            @Override
            public void jobletFinish(JobStatus jobStatus) {
                try {
                    TransactionProvider factory = (TransactionProvider) (jobletContext.getApplicationContext()
                            .getApplicationObject());
                    ITransactionManager txnManager = factory.getTransactionManager();
                    TransactionContext txnContext = txnManager.getTransactionContext(txnId);
                    txnContext.setTransactionType(transactionalWrite ? TransactionType.READ_WRITE
                            : TransactionType.READ);
                    txnManager.completedTransaction(txnContext, !(jobStatus == JobStatus.FAILURE));
                } catch (ACIDException e) {
                    throw new Error(e);
                }
            }

            @Override
            public void jobletStart() {

            }

        };
    }
}
