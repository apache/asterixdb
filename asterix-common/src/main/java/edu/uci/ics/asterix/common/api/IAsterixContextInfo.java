package edu.uci.ics.asterix.common.api;

import edu.uci.ics.hyracks.api.application.ICCApplicationContext;

public interface IAsterixContextInfo {

    /**
     * Returns an instance of the implementation for ICCApplicationContext.
     * 
     * @return ICCApplicationContext implementation instance
     */
    public ICCApplicationContext getCCApplicationContext();
}
