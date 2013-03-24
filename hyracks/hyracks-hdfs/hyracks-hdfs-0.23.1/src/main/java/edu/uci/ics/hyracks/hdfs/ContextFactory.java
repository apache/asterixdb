package edu.uci.ics.hyracks.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

/**
 * The wrapper to generate TaskTattemptContext
 */
public class ContextFactory {

    public TaskAttemptContext createContext(Configuration conf, TaskAttemptID tid) throws HyracksDataException {
        try {
            return new TaskAttemptContextImpl(conf, tid);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    public TaskAttemptContext createContext(Configuration conf, int partition) throws HyracksDataException {
        try {
            TaskAttemptID tid = new TaskAttemptID("", 0, TaskType.REDUCE, partition, 0);
            return new TaskAttemptContextImpl(conf, tid);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    public JobContext createJobContext(Configuration conf) {
        return new JobContextImpl(conf, new JobID("0", 0));
    }

}
