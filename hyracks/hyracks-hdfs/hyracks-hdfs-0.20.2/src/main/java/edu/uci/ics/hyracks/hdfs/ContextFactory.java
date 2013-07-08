/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

/**
 * The wrapper to generate TaskTattemptContext
 */
public class ContextFactory {

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public TaskAttemptContext createContext(Configuration conf, TaskAttemptID tid) throws HyracksDataException {
        try {
            return new Mapper().new Context(conf, tid, null, null, null, null, null);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    public TaskAttemptContext createContext(Configuration conf, int partition) throws HyracksDataException {
        try {
            TaskAttemptID tid = new TaskAttemptID("", 0, true, partition, 0);
            return new TaskAttemptContext(conf, tid);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    public JobContext createJobContext(Configuration conf) {
        return new JobContext(conf, new JobID("0", 0));
    }

}
