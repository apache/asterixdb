package edu.uci.ics.asterix.common.context;

import edu.uci.ics.asterix.common.api.INodeApplicationState;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionProvider;

public class NodeApplicationState implements INodeApplicationState {

    private AsterixAppRuntimeContext appRuntimeContext;
    private TransactionProvider provider;
    
    @Override
    public AsterixAppRuntimeContext getApplicationRuntimeContext() {
        return appRuntimeContext;
    }

    @Override
    public void setApplicationRuntimeContext(AsterixAppRuntimeContext context) {
        this.appRuntimeContext = context;
    }

    @Override
    public TransactionProvider getTransactionProvider() {
        return provider;
    }

    @Override
    public void setTransactionProvider(TransactionProvider provider) {
        this.provider = provider;
    }

}
