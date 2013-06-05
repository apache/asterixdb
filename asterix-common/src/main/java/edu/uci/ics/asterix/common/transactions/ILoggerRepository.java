package edu.uci.ics.asterix.common.transactions;

import edu.uci.ics.asterix.common.exceptions.ACIDException;

public interface ILoggerRepository {

    public ILogger getIndexLogger(long resourceId, byte resourceType) throws ACIDException;
}
