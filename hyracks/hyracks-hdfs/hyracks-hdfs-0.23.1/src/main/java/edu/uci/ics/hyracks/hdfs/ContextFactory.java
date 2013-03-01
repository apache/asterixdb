package edu.uci.ics.hyracks.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

/**
 * The wrapper to generate TaskTattemptContext
 */
public class ContextFactory {

    public TaskAttemptContext createContext(Configuration conf, InputSplit split) throws HyracksDataException {
        try {
            return new TaskAttemptContextImpl(conf, new TaskAttemptID());
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

}
