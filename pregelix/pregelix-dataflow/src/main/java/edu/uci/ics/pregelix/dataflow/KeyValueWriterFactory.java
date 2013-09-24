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
package edu.uci.ics.pregelix.dataflow;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.util.ReflectionUtils;
import edu.uci.ics.hyracks.hdfs.ContextFactory;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriter;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriterFactory;
import edu.uci.ics.hyracks.hdfs2.dataflow.ConfFactory;
import edu.uci.ics.pregelix.api.util.ResetableByteArrayInputStream;

/**
 * @author yingyib
 */
@SuppressWarnings("rawtypes")
public class KeyValueWriterFactory implements ITupleWriterFactory {
    private static final long serialVersionUID = 1L;
    private ConfFactory confFactory;

    public KeyValueWriterFactory(ConfFactory confFactory) {
        this.confFactory = confFactory;
    }

    @Override
    public ITupleWriter getTupleWriter(IHyracksTaskContext ctx, final int partition, final int nPartition)
            throws HyracksDataException {
        return new ITupleWriter() {
            private SequenceFileOutputFormat sequenceOutputFormat = new SequenceFileOutputFormat();
            private Writable key;
            private Writable value;
            private ResetableByteArrayInputStream bis = new ResetableByteArrayInputStream();
            private DataInput dis = new DataInputStream(bis);
            private RecordWriter recordWriter;
            private ContextFactory ctxFactory = new ContextFactory();
            private TaskAttemptContext context;

            @Override
            public void open(DataOutput output) throws HyracksDataException {
                try {
                    Job job = confFactory.getConf();
                    context = ctxFactory.createContext(job.getConfiguration(), partition);
                    recordWriter = sequenceOutputFormat.getRecordWriter(context);
                    Class<?> keyClass = context.getOutputKeyClass();
                    Class<?> valClass = context.getOutputValueClass();
                    key = (Writable) ReflectionUtils.createInstance(keyClass);
                    value = (Writable) ReflectionUtils.createInstance(valClass);
                } catch (Exception e) {
                    throw new HyracksDataException(e);
                }
            }

            @SuppressWarnings("unchecked")
            @Override
            public void write(DataOutput output, ITupleReference tuple) throws HyracksDataException {
                try {
                    byte[] data = tuple.getFieldData(0);
                    int fieldStart = tuple.getFieldStart(0);
                    bis.setByteArray(data, fieldStart);
                    key.readFields(dis);
                    data = tuple.getFieldData(1);
                    fieldStart = tuple.getFieldStart(1);
                    bis.setByteArray(data, fieldStart);
                    value.readFields(dis);
                    recordWriter.write(key, value);
                } catch (Exception e) {
                    throw new HyracksDataException(e);
                }
            }

            @Override
            public void close(DataOutput output) throws HyracksDataException {
                try {
                    recordWriter.close(context);
                } catch (Exception e) {
                    throw new HyracksDataException(e);
                }
            }

        };
    }
}
