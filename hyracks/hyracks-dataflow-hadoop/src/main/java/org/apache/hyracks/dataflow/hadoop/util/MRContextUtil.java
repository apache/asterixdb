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
package org.apache.hyracks.dataflow.hadoop.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.apache.hadoop.mapreduce.lib.reduce.WrappedReducer;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.apache.hadoop.mapreduce.task.ReduceContextImpl;

import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * The wrapper to generate TaskTattemptContext
 */
public class MRContextUtil {
    //	public static Reducer.Context = create

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public Mapper.Context createMapContext(Configuration conf, TaskAttemptID taskid, RecordReader reader,
            RecordWriter writer, OutputCommitter committer, StatusReporter reporter, InputSplit split) {
        return new WrappedMapper().getMapContext(new MapContextImpl(conf, taskid, reader, writer, committer, reporter,
                split));
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public Reducer.Context createReduceContext(Configuration conf, TaskAttemptID taskid, RawKeyValueIterator input,
            Counter inputKeyCounter, Counter inputValueCounter, RecordWriter output, OutputCommitter committer,
            StatusReporter reporter, RawComparator comparator, Class keyClass, Class valueClass)
            throws HyracksDataException {
        try {
            return new WrappedReducer().getReducerContext(new ReduceContextImpl(conf, taskid, input, inputKeyCounter,
                    inputValueCounter, output, committer, reporter, comparator, keyClass, valueClass));
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }
}