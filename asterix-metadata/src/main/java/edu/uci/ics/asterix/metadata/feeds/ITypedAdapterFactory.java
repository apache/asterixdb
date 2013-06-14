package edu.uci.ics.asterix.metadata.feeds;

import edu.uci.ics.asterix.om.types.ARecordType;

public interface ITypedAdapterFactory extends IAdapterFactory {

    public ARecordType getAdapterOutputType();
}
