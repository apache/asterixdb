package edu.uci.ics.asterix.common.transactions;


public interface ITransactionSubsystem {

    public ILogManager getLogManager();

    public ILockManager getLockManager();

    public ITransactionManager getTransactionManager();

    public IRecoveryManager getRecoveryManager();

    public TransactionalResourceManagerRepository getTransactionalResourceRepository();

    public ILoggerRepository getTreeLoggerRepository();

    public IAsterixAppRuntimeContextProvider getAsterixAppRuntimeContextProvider();

    public String getId();
}
