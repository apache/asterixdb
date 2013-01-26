package edu.uci.ics.asterix.transaction.management.resource;

import java.io.Serializable;

import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;

public interface ILocalResourceMetadata extends Serializable {

    public ILSMIndex createIndexInstance();
    
}
