package edu.uci.ics.asterix.metadata.feeds;

import java.util.Map;

import edu.uci.ics.asterix.om.types.ARecordType;

public interface ITypedAdapterFactory extends IAdapterFactory {

    public ARecordType getAdapterOutputType();

    public void configure(Map<String, String> configuration) throws Exception;
}
