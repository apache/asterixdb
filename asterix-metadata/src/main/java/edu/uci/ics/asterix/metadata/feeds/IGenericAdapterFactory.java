package edu.uci.ics.asterix.metadata.feeds;

import java.util.Map;

import edu.uci.ics.asterix.om.types.ARecordType;

public interface IGenericAdapterFactory extends IAdapterFactory {

    public void configure(Map<String, String> configuration, ARecordType outputType) throws Exception;

}
