package edu.uci.ics.asterix.common.api;

import edu.uci.ics.asterix.common.context.AsterixAppRuntimeContext;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionProvider;

public interface INodeApplicationState {
    public AsterixAppRuntimeContext getApplicationRuntimeContext();
    
    public void setApplicationRuntimeContext(AsterixAppRuntimeContext context);
    
    public TransactionProvider getTransactionProvider();
    
    public void setTransactionProvider(TransactionProvider provider);
}
