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
/*
 * Copyright 2009-2010 University of California, Irvine
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
package edu.uci.ics.hyracks.dataflow.hadoop;

import java.io.IOException;
import java.nio.ByteBuffer;
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

import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.hadoop.util.DatatypeHelper;
import edu.uci.ics.hyracks.dataflow.hadoop.util.InputSplitsProxy;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

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
            JobContext context = new JobContext(conf, null);
            org.apache.hadoop.mapreduce.InputFormat inputFormat = (org.apache.hadoop.mapreduce.InputFormat) ReflectionUtils
                    .newInstance(context.getInputFormatClass(), conf);
            TaskAttemptContext taskAttemptContext = new org.apache.hadoop.mapreduce.TaskAttemptContext(jobConf, null);
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
                        JobContext context = new JobContext(conf, null);
                        org.apache.hadoop.mapreduce.InputFormat inputFormat = (org.apache.hadoop.mapreduce.InputFormat) ReflectionUtils
                                .newInstance(context.getInputFormatClass(), conf);
                        TaskAttemptContext taskAttemptContext = new org.apache.hadoop.mapreduce.TaskAttemptContext(
                                jobConf, null);
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
                    ByteBuffer outBuffer = ctx.allocateFrame();
                    FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
                    appender.reset(outBuffer, true);
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
                            if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                                FrameUtils.flushFrame(outBuffer, writer);
                                appender.reset(outBuffer, true);
                                if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                                    throw new HyracksDataException("Record size (" + tb.getSize() + ") larger than frame size (" + outBuffer.capacity() + ")");
                                }
                            }
                        }
                        if (appender.getTupleCount() > 0) {
                            FrameUtils.flushFrame(outBuffer, writer);
                        }
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
