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
package org.apache.hyracks.dataflow.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileRecordReader;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.constraints.PartitionConstraintHelper;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.hadoop.util.DatatypeHelper;
import org.apache.hyracks.dataflow.hadoop.util.InputSplitsProxy;
import org.apache.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import org.apache.hyracks.hdfs.ContextFactory;

public class HadoopReadOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private String inputFormatClassName;
    private Map<String, String> jobConfMap;
    private InputSplitsProxy inputSplitsProxy;
    private transient JobConf jobConf;

    public JobConf getJobConf() {
        if (jobConf == null) {
            jobConf = DatatypeHelper.map2JobConf(jobConfMap);
        }
        return jobConf;
    }

    public HadoopReadOperatorDescriptor(JobConf jobConf, JobSpecification spec, Object[] splits) throws IOException {
        super(spec, 0, 1);
        this.jobConfMap = DatatypeHelper.jobConf2Map(jobConf);
        InputFormat inputFormat = jobConf.getInputFormat();
        RecordReader recordReader;
        try {
            recordReader = getRecordReader(DatatypeHelper.map2JobConf(jobConfMap), splits[0]);
        } catch (Exception e) {
            throw new IOException(e);
        }
        recordDescriptors[0] = DatatypeHelper.createKeyValueRecordDescriptor((Class<? extends Writable>) recordReader
                .createKey().getClass(), (Class<? extends Writable>) recordReader.createValue().getClass());
        PartitionConstraintHelper.addPartitionCountConstraint(spec, this, splits.length);
        inputSplitsProxy = new InputSplitsProxy(jobConf, splits);
        this.inputFormatClassName = inputFormat.getClass().getName();
    }

    private RecordReader getRecordReader(JobConf conf, Object inputSplit) throws ClassNotFoundException, IOException,
            InterruptedException {
        RecordReader hadoopRecordReader = null;
        if (conf.getUseNewMapper()) {
            JobContext context = new ContextFactory().createJobContext(conf);
            org.apache.hadoop.mapreduce.InputFormat inputFormat = (org.apache.hadoop.mapreduce.InputFormat) ReflectionUtils
                    .newInstance(context.getInputFormatClass(), conf);
            TaskAttemptContext taskAttemptContext = new ContextFactory().createContext(jobConf, null);
            hadoopRecordReader = (RecordReader) inputFormat.createRecordReader(
                    (org.apache.hadoop.mapreduce.InputSplit) inputSplit, taskAttemptContext);
        } else {
            Class inputFormatClass = conf.getInputFormat().getClass();
            InputFormat inputFormat = (InputFormat) ReflectionUtils.newInstance(inputFormatClass, conf);
            hadoopRecordReader = (RecordReader) inputFormat.getRecordReader(
                    (org.apache.hadoop.mapred.InputSplit) inputSplit, conf, createReporter());
        }
        return hadoopRecordReader;
    }

    public Object[] getInputSplits() throws InstantiationException, IllegalAccessException, IOException {
        return inputSplitsProxy.toInputSplits(getJobConf());
    }

    protected Reporter createReporter() {
        return new Reporter() {
            @Override
            public Counter getCounter(Enum<?> name) {
                return null;
            }

            @Override
            public Counter getCounter(String group, String name) {
                return null;
            }

            @Override
            public InputSplit getInputSplit() throws UnsupportedOperationException {
                return null;
            }

            @Override
            public void incrCounter(Enum<?> key, long amount) {

            }

            @Override
            public void incrCounter(String group, String counter, long amount) {

            }

            @Override
            public void progress() {

            }

            @Override
            public void setStatus(String status) {

            }

            @Override
            public float getProgress() {
                // TODO Auto-generated method stub
                return 0;
            }
        };
    }

    @SuppressWarnings("deprecation")
    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            final IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions)
            throws HyracksDataException {
        return new AbstractUnaryOutputSourceOperatorNodePushable() {
            @Override
            public void initialize() throws HyracksDataException {
                try {
                    JobConf conf = DatatypeHelper.map2JobConf((HashMap) jobConfMap);
                    Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
                    conf.setClassLoader(this.getClass().getClassLoader());
                    RecordReader hadoopRecordReader;
                    Object key;
                    Object value;
                    Object[] splits = inputSplitsProxy.toInputSplits(conf);
                    Object inputSplit = splits[partition];

                    if (conf.getUseNewMapper()) {
                        JobContext context = new ContextFactory().createJobContext(conf);
                        org.apache.hadoop.mapreduce.InputFormat inputFormat = (org.apache.hadoop.mapreduce.InputFormat) ReflectionUtils
                                .newInstance(context.getInputFormatClass(), conf);
                        TaskAttemptContext taskAttemptContext = new ContextFactory().createContext(jobConf, null);
                        hadoopRecordReader = (RecordReader) inputFormat.createRecordReader(
                                (org.apache.hadoop.mapreduce.InputSplit) inputSplit, taskAttemptContext);
                    } else {
                        Class inputFormatClass = conf.getInputFormat().getClass();
                        InputFormat inputFormat = (InputFormat) ReflectionUtils.newInstance(inputFormatClass, conf);
                        hadoopRecordReader = (RecordReader) inputFormat.getRecordReader(
                                (org.apache.hadoop.mapred.InputSplit) inputSplit, conf, createReporter());
                    }

                    Class inputKeyClass;
                    Class inputValueClass;
                    if (hadoopRecordReader instanceof SequenceFileRecordReader) {
                        inputKeyClass = ((SequenceFileRecordReader) hadoopRecordReader).getKeyClass();
                        inputValueClass = ((SequenceFileRecordReader) hadoopRecordReader).getValueClass();
                    } else {
                        inputKeyClass = hadoopRecordReader.createKey().getClass();
                        inputValueClass = hadoopRecordReader.createValue().getClass();
                    }

                    key = hadoopRecordReader.createKey();
                    value = hadoopRecordReader.createValue();
                    FrameTupleAppender appender = new FrameTupleAppender(new VSizeFrame(ctx));
                    RecordDescriptor outputRecordDescriptor = DatatypeHelper.createKeyValueRecordDescriptor(
                            (Class<? extends Writable>) hadoopRecordReader.createKey().getClass(),
                            (Class<? extends Writable>) hadoopRecordReader.createValue().getClass());
                    int nFields = outputRecordDescriptor.getFieldCount();
                    ArrayTupleBuilder tb = new ArrayTupleBuilder(nFields);
                    writer.open();
                    try {
                        while (hadoopRecordReader.next(key, value)) {
                            tb.reset();
                            switch (nFields) {
                                case 2:
                                    tb.addField(outputRecordDescriptor.getFields()[0], key);
                                case 1:
                                    tb.addField(outputRecordDescriptor.getFields()[1], value);
                            }
                            FrameUtils
                                    .appendToWriter(writer, appender, tb.getFieldEndOffsets(), tb.getByteArray(),
                                            0, tb.getSize());
                        }
                        appender.flush(writer, true);
                    } catch (Exception e) {
                        writer.fail();
                        throw new HyracksDataException(e);
                    } finally {
                        writer.close();
                    }
                    hadoopRecordReader.close();
                } catch (InstantiationException e) {
                    throw new HyracksDataException(e);
                } catch (IllegalAccessException e) {
                    throw new HyracksDataException(e);
                } catch (ClassNotFoundException e) {
                    throw new HyracksDataException(e);
                } catch (InterruptedException e) {
                    throw new HyracksDataException(e);
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
        };
    }
}
