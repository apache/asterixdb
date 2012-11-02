package edu.uci.ics.hivesterix.runtime.operator.filescan;

import java.io.InputStream;

import org.apache.hadoop.mapred.FileSplit;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParser;

public abstract class AbstractHiveTupleParser implements ITupleParser {

    @Override
    public void parse(InputStream in, IFrameWriter writer) throws HyracksDataException {
        // empty implementation
    }

    /**
     * method for parsing HDFS file split
     * 
     * @param split
     * @param writer
     */
    abstract public void parse(FileSplit split, IFrameWriter writer) throws HyracksDataException;

}
