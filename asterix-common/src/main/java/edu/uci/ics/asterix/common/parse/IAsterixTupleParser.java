package edu.uci.ics.asterix.common.parse;

import java.util.Map;

import edu.uci.ics.hyracks.dataflow.std.file.ITupleParser;

public interface IAsterixTupleParser extends ITupleParser{

    public void configure(Map<String, String> configuration);
    
}
