package edu.uci.ics.asterix.common.transactions;

public interface ILoggerRepository {

    public  ILogger getIndexLogger(long resourceId, byte resourceType);
}
