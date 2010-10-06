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
import org.apache.hadoop.util.ReflectionUtils;

import edu.uci.ics.hyracks.api.constraints.PartitionCountConstraint;
import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
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

    public HadoopReadOperatorDescriptor(JobConf jobConf, JobSpecification spec) throws IOException {
        super(spec, 0, 1);
        this.jobConfMap = DatatypeHelper.jobConf2Map(jobConf);
        InputFormat inputFormat = jobConf.getInputFormat();
        InputSplit[] splits = inputFormat.getSplits(jobConf, jobConf.getNumMapTasks());
        RecordReader recordReader = inputFormat.getRecordReader(splits[0], jobConf, createReporter());
        recordDescriptors[0] = DatatypeHelper.createKeyValueRecordDescriptor((Class<? extends Writable>) recordReader
                .createKey().getClass(), (Class<? extends Writable>) recordReader.createValue().getClass());
        this.setPartitionConstraint(new PartitionCountConstraint(splits.length));
        inputSplitsProxy = new InputSplitsProxy(splits);
        this.inputFormatClassName = inputFormat.getClass().getName();
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
    public IOperatorNodePushable createPushRuntime(final IHyracksContext ctx, IOperatorEnvironment env,
            final IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions)
            throws HyracksDataException {
        return new AbstractUnaryOutputSourceOperatorNodePushable() {
            @Override
            public void initialize() throws HyracksDataException {
                try {
                    JobConf conf = DatatypeHelper.map2JobConf((HashMap) jobConfMap);
                    RecordReader hadoopRecordReader;
                    Writable key;
                    Writable value;
                    InputSplit[] splits = inputSplitsProxy.toInputSplits(conf);
                    InputSplit inputSplit = splits[partition];
                    Class inputFormatClass = Class.forName(inputFormatClassName);
                    InputFormat inputFormat = (InputFormat) ReflectionUtils.newInstance(inputFormatClass, conf);
                    hadoopRecordReader = (RecordReader) inputFormat.getRecordReader(inputSplit, conf, createReporter());
                    Class inputKeyClass;
                    Class inputValueClass;
                    if (hadoopRecordReader instanceof SequenceFileRecordReader) {
                        inputKeyClass = ((SequenceFileRecordReader) hadoopRecordReader).getKeyClass();
                        inputValueClass = ((SequenceFileRecordReader) hadoopRecordReader).getValueClass();
                    } else {
                        inputKeyClass = hadoopRecordReader.createKey().getClass();
                        inputValueClass = hadoopRecordReader.createValue().getClass();
                    }
                    key = (Writable) inputKeyClass.newInstance();
                    value = (Writable) inputValueClass.newInstance();
                    ByteBuffer outBuffer = ctx.getResourceManager().allocateFrame();
                    FrameTupleAppender appender = new FrameTupleAppender(ctx);
                    appender.reset(outBuffer, true);
                    RecordDescriptor outputRecordDescriptor = recordDescProvider.getOutputRecordDescriptor(
                            getOperatorId(), 0);
                    int nFields = outputRecordDescriptor.getFields().length;
                    ArrayTupleBuilder tb = new ArrayTupleBuilder(nFields);
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
                                throw new IllegalStateException();
                            }
                        }
                    }
                    if (appender.getTupleCount() > 0) {
                        FrameUtils.flushFrame(outBuffer, writer);
                    }
                    writer.close();
                    hadoopRecordReader.close();
                } catch (InstantiationException e) {
                    throw new HyracksDataException(e);
                } catch (IllegalAccessException e) {
                    throw new HyracksDataException(e);
                } catch (ClassNotFoundException e) {
                    throw new HyracksDataException(e);
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
        };
    }
}