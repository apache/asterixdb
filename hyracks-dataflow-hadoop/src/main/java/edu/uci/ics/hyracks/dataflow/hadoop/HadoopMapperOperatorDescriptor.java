/*
 * Copyright 2009-2010 by The Regents of the University of California
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

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileRecordReader;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.ReflectionUtils;

import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.dataflow.IOpenableDataWriter;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.SerializingDataWriter;
import edu.uci.ics.hyracks.dataflow.hadoop.util.DatatypeHelper;
import edu.uci.ics.hyracks.dataflow.hadoop.util.IHadoopClassFactory;
import edu.uci.ics.hyracks.dataflow.hadoop.util.InputSplitsProxy;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.base.IOpenableDataWriterOperator;
import edu.uci.ics.hyracks.dataflow.std.util.DeserializedOperatorNodePushable;

public class HadoopMapperOperatorDescriptor<K1, V1, K2, V2> extends AbstractHadoopOperatorDescriptor {

    private class MapperBaseOperator {
        protected OutputCollector<K2, V2> output;
        protected Reporter reporter;
        protected Mapper<K1, V1, K2, V2> mapper;
        protected int partition;
        protected JobConf conf;
        protected IOpenableDataWriter<Object[]> writer;

        public MapperBaseOperator(int partition) {
            this.partition = partition;
        }

        protected void initializeMapper() throws HyracksDataException {
            jobConf = getJobConf();
            populateCache(jobConf);
            Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
            try {
                mapper = createMapper();
            } catch (Exception e) {
                throw new HyracksDataException(e);
            }
            conf = new JobConf(jobConf);
            conf.setClassLoader(jobConf.getClassLoader());
            reporter = createReporter();
        }

        protected void map(Object[] data) throws HyracksDataException {
            try {
                mapper.map((K1) data[0], (V1) data[1], output, reporter);
            } catch (IOException e) {
                throw new HyracksDataException(e);
            } catch (RuntimeException re) {
                System.out.println(" Runtime exceptione encoutered for row :" + data[0] + ": " + data[1]);
                re.printStackTrace();
            }
        }

        protected void closeMapper() throws HyracksDataException {
            try {
                mapper.close();
            } catch (IOException ioe) {
                throw new HyracksDataException(ioe);
            }
        }

    }

    private class MapperOperator extends MapperBaseOperator implements IOpenableDataWriterOperator {

        public MapperOperator(int partition) {
            super(partition);
        };

        @Override
        public void close() throws HyracksDataException {
            super.closeMapper();
            writer.close();
        }

        @Override
        public void open() throws HyracksDataException {
            initializeMapper();
            mapper.configure(conf);
            writer.open();
            output = new DataWritingOutputCollector<K2, V2>(writer);
        }

        @Override
        public void writeData(Object[] data) throws HyracksDataException {
            super.map(data);
        }

        public void setDataWriter(int index, IOpenableDataWriter<Object[]> writer) {
            if (index != 0) {
                throw new IllegalArgumentException();
            }
            this.writer = writer;
        }
    }

    private class ReaderMapperOperator extends MapperBaseOperator {

        public ReaderMapperOperator(int partition, IOpenableDataWriter writer) throws HyracksDataException {
            super(partition);
            output = new DataWritingOutputCollector<K2, V2>(writer);
            this.writer = writer;
            this.writer.open();
        }

        protected void updateConfWithSplit(JobConf conf) {
            try {
                if (inputSplits == null) {
                    inputSplits = inputSplitsProxy.toInputSplits(conf);
                }
                Object splitRead = inputSplits[partition];
                if (splitRead instanceof FileSplit) {
                    conf.set("map.input.file", ((FileSplit) splitRead).getPath().toString());
                    conf.setLong("map.input.start", ((FileSplit) splitRead).getStart());
                    conf.setLong("map.input.length", ((FileSplit) splitRead).getLength());
                } else if ( splitRead instanceof org.apache.hadoop.mapreduce.lib.input.FileSplit) {
                    conf.set("map.input.file", ((org.apache.hadoop.mapreduce.lib.input.FileSplit) splitRead).getPath().toString());
                    conf.setLong("map.input.start", ((org.apache.hadoop.mapreduce.lib.input.FileSplit) splitRead).getStart());
                    conf.setLong("map.input.length", ((org.apache.hadoop.mapreduce.lib.input.FileSplit) splitRead).getLength());
                }
            } catch (Exception e) {
                e.printStackTrace();
                // we do not throw the exception here as we are setting additional parameters that may not be 
                // required by the mapper. If they are  indeed required,  the configure method invoked on the mapper
                // shall report an exception because of the missing parameters. 
            }
        }

        public void mapInput() throws HyracksDataException, InterruptedException, ClassNotFoundException {
            try {
                Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
                initializeMapper();
                updateConfWithSplit(conf);
                mapper.configure(conf);
                conf.setClassLoader(this.getClass().getClassLoader());
                Object reader;
                Object key = null;
                Object value = null;
                Object inputSplit = inputSplits[partition];
                reader = getRecordReader(conf, inputSplit);
                Class inputKeyClass = null;
                Class inputValueClass = null;
                Object[] data = new Object[2];
                  
                if(conf.getUseNewMapper()){
                    org.apache.hadoop.mapreduce.RecordReader newReader = (org.apache.hadoop.mapreduce.RecordReader)reader;
                    newReader.initialize((org.apache.hadoop.mapreduce.InputSplit)inputSplit, new TaskAttemptContext(conf, new TaskAttemptID()));
                    while(newReader.nextKeyValue()){
                        data[0] = newReader.getCurrentKey();
                        if(data[0] == null) {
                            data[0] = NullWritable.get();
                        }
                        data[1] = newReader.getCurrentValue();
                        super.map(data);
                    }
                } else {
                    RecordReader oldReader = (RecordReader)reader;
                    if (reader instanceof SequenceFileRecordReader) {
                        inputKeyClass = ((SequenceFileRecordReader) oldReader).getKeyClass();
                        inputValueClass = ((SequenceFileRecordReader) oldReader).getValueClass();
                    } else {
                        inputKeyClass = oldReader.createKey().getClass();
                        inputValueClass = oldReader.createValue().getClass();
                    }
                    key = oldReader.createKey();
                    value = oldReader.createValue();
                    while (oldReader.next(key, value)) {
                        data[0] = key;
                        data[1] = value;
                        super.map(data);
                    }
                    oldReader.close();
                }
                
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }

        }

        public void close() throws HyracksDataException {
            super.closeMapper();
            writer.close();
        }
    }

    private static final long serialVersionUID = 1L;
    private Class<? extends Mapper> mapperClass;
    private InputSplitsProxy inputSplitsProxy;
    private transient Object[] inputSplits;
    private boolean selfRead = false;

    private void initializeSplitInfo(Object[] splits) throws IOException {
        jobConf = super.getJobConf();
        InputFormat inputFormat = jobConf.getInputFormat();
        inputSplitsProxy = new InputSplitsProxy(jobConf,splits);
    }

    public HadoopMapperOperatorDescriptor(JobSpecification spec, JobConf jobConf, IHadoopClassFactory hadoopClassFactory)
            throws IOException {
        super(spec, 1, getRecordDescriptor(jobConf, hadoopClassFactory), jobConf, hadoopClassFactory);
    }

    public HadoopMapperOperatorDescriptor(JobSpecification spec, JobConf jobConf, Object[] splits,
            IHadoopClassFactory hadoopClassFactory) throws IOException {
        super(spec, 0, getRecordDescriptor(jobConf, hadoopClassFactory), jobConf, hadoopClassFactory);
        initializeSplitInfo(splits);
        this.selfRead = true;
    }

    public static RecordDescriptor getRecordDescriptor(JobConf conf, IHadoopClassFactory hadoopClassFactory) {
        RecordDescriptor recordDescriptor = null;
        String mapOutputKeyClassName = conf.getMapOutputKeyClass().getName();
        String mapOutputValueClassName = conf.getMapOutputValueClass().getName();
        try {
            if (hadoopClassFactory == null) {
                recordDescriptor = DatatypeHelper.createKeyValueRecordDescriptor((Class<? extends Writable>) Class
                        .forName(mapOutputKeyClassName), (Class<? extends Writable>) Class
                        .forName(mapOutputValueClassName));
            } else {
                recordDescriptor = DatatypeHelper.createKeyValueRecordDescriptor(
                        (Class<? extends Writable>) hadoopClassFactory.loadClass(mapOutputKeyClassName),
                        (Class<? extends Writable>) hadoopClassFactory.loadClass(mapOutputValueClassName));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return recordDescriptor;
    }

    private Mapper<K1, V1, K2, V2> createMapper() throws Exception {
        if (mapperClass != null) {
            return mapperClass.newInstance();
        } else {
            String mapperClassName = super.getJobConf().getMapperClass().getName();
            Object mapper = getHadoopClassFactory().createMapper(mapperClassName);
            mapperClass = (Class<? extends Mapper>) mapper.getClass();
            return (Mapper) mapper;
        }
    }

    private Object getRecordReader(JobConf conf, Object inputSplit) throws ClassNotFoundException, IOException, InterruptedException {
        if(conf.getUseNewMapper()){
            JobContext context = new JobContext(conf,null);
            org.apache.hadoop.mapreduce.InputFormat inputFormat = (org.apache.hadoop.mapreduce.InputFormat) ReflectionUtils.newInstance(context.getInputFormatClass(), conf);
            TaskAttemptContext taskAttemptContext = new org.apache.hadoop.mapreduce.TaskAttemptContext(jobConf,new TaskAttemptID());
            return inputFormat.createRecordReader((org.apache.hadoop.mapreduce.InputSplit)inputSplit,taskAttemptContext);
        } else {
            Class inputFormatClass = conf.getInputFormat().getClass();
            InputFormat inputFormat = (InputFormat) ReflectionUtils.newInstance(inputFormatClass, conf);
            return inputFormat.getRecordReader((org.apache.hadoop.mapred.InputSplit)inputSplit, conf, super.createReporter());
        }
    }
    
    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksContext ctx, IOperatorEnvironment env,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {

        JobConf conf = getJobConf();
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
        try {
            if (selfRead) {
                RecordDescriptor recordDescriptor = null;
                if (inputSplits == null) {
                    inputSplits = inputSplitsProxy.toInputSplits(conf);
                }
                Object reader = getRecordReader(conf,inputSplits[partition]);
                if(conf.getUseNewMapper()) {
                    org.apache.hadoop.mapreduce.RecordReader newReader = (org.apache.hadoop.mapreduce.RecordReader)reader;
                    newReader.initialize((org.apache.hadoop.mapreduce.InputSplit)inputSplits[partition], new TaskAttemptContext(conf, new TaskAttemptID()));
                    newReader.nextKeyValue();
                    Object key = newReader.getCurrentKey();
                    Class keyClass = null;
                    if (key == null) {
                        keyClass = Class.forName("org.apache.hadoop.io.NullWritable");
                    }
                    recordDescriptor = DatatypeHelper.createKeyValueRecordDescriptor((Class<? extends Writable>) keyClass,
                            (Class<? extends Writable>) newReader.getCurrentValue().getClass());
                } else {
                RecordReader oldReader = (RecordReader)reader;    
                recordDescriptor = DatatypeHelper.createKeyValueRecordDescriptor((Class<? extends Writable>) oldReader
                        .createKey().getClass(), (Class<? extends Writable>) oldReader.createValue().getClass());
                }
                return createSelfReadingMapper(ctx, env, recordDescriptor, partition);
            } else {
                return new DeserializedOperatorNodePushable(ctx, new MapperOperator(partition), recordDescProvider
                        .getInputRecordDescriptor(this.odId, 0));
            }
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    private IOperatorNodePushable createSelfReadingMapper(final IHyracksContext ctx, IOperatorEnvironment env,
            final RecordDescriptor recordDescriptor, final int partition) {
        return new AbstractUnaryOutputSourceOperatorNodePushable() {
            @Override
            public void initialize() throws HyracksDataException {
                SerializingDataWriter writer = new SerializingDataWriter(ctx, recordDescriptor, this.writer);
                ReaderMapperOperator readMapOp = new ReaderMapperOperator(partition, writer);
                try {
                    readMapOp.mapInput();
                } catch (Exception e) {
                    throw new HyracksDataException(e);
                } 
                readMapOp.close();
            }
        };
    }

    public Class<? extends Mapper> getMapperClass() {
        return mapperClass;
    }
}
