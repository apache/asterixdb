package edu.uci.ics.hyracks.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

/**
 * The wrapper to generate TaskTattemptContext
 */
public class ContextFactory {

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public TaskAttemptContext createContext(Configuration conf, InputSplit split) throws HyracksDataException {
        try {
            return new Mapper().new Context(conf, new TaskAttemptID(), null, null, null, null, split);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

}
