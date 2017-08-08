/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * The wrapper to generate TaskTattemptContext
 */
public class ContextFactory {

    public TaskAttemptContext createContext(Configuration conf, TaskAttemptID tid) throws HyracksDataException {
        try {
            return new TaskAttemptContextImpl(conf, tid);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    public TaskAttemptContext createContext(Configuration conf, int partition) throws HyracksDataException {
        try {
            TaskAttemptID tid = new TaskAttemptID("", 0, TaskType.REDUCE, partition, 0);
            return new TaskAttemptContextImpl(conf, tid);
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }

    public JobContext createJobContext(Configuration conf) {
        return new JobContextImpl(conf, new JobID("0", 0));
    }

}
