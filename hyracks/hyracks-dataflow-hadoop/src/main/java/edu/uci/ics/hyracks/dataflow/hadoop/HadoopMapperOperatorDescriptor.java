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
package edu.uci.ics.hyracks.dataflow.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileRecordReader;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.ReflectionUtils;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOpenableDataWriter;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
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
        protected Object mapper;
        // protected Mapper<K1, V1, K2, V2> mapper;
        protected int partition;
        protected JobConf conf;
        protected IOpenableDataWriter<Object[]> writer;
        protected boolean newMapreduceLib = false;
        org.apache.hadoop.mapreduce.Mapper.Context context;

        public MapperBaseOperator(int partition) {
            this.partition = partition;
        }

        protected void initializeMapper() throws HyracksDataException {
            Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
            jobConf = getJobConf();
            populateCache(jobConf);
            conf = new JobConf(jobConf);
            conf.setClassLoader(jobConf.getClassLoader());
            reporter = createReporter();
        }

        protected void map(Object[] data) throws HyracksDataException {
            try {
                if (!conf.getUseNewMapper()) {
                    ((org.apache.hadoop.mapred.Mapper) mapper).map((K1) data[0], (V1) data[1], output, reporter);
                } else
                    throw new IllegalStateException(
                            " Incorrect map method called for MapReduce code written using mapreduce package");
            } catch (IOException e) {
                throw new HyracksDataException(e);
            } catch (RuntimeException re) {
                System.out.println(" Runtime exceptione encoutered for row :" + data[0] + ": " + data[1]);
                re.printStackTrace();
            }
        }

        protected void closeMapper() throws HyracksDataException {
            try {
                if (!conf.getUseNewMapper()) {
                    ((org.apache.hadoop.mapred.Mapper) mapper).close();
                } else {
                    // do nothing. closing the mapper is handled internally by
                    // run method on context.
                }
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
        public void fail() throws HyracksDataException {
            writer.fail();
        }

        @Override
        public void open() throws HyracksDataException {
            initializeMapper();
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

        protected void initializeMapper() throws HyracksDataException {
            super.initializeMapper();
            try {
                mapper = createMapper(conf);
            } catch (Exception e) {
                throw new HyracksDataException(e);
            }
            if (!conf.getUseNewMapper()) {
                ((org.apache.hadoop.mapred.Mapper) mapper).configure(conf);
            }
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
                } else if (splitRead instanceof org.apache.hadoop.mapreduce.lib.input.FileSplit) {
                    conf.set("map.input.file", ((org.apache.hadoop.mapreduce.lib.input.FileSplit) splitRead).getPath()
                            .toString());
                    conf.setLong("map.input.start",
                            ((org.apache.hadoop.mapreduce.lib.input.FileSplit) splitRead).getStart());
                    conf.setLong("map.input.length",
                            ((org.apache.hadoop.mapreduce.lib.input.FileSplit) splitRead).getLength());
                }
            } catch (Exception e) {
                e.printStackTrace();
                // we do not throw the exception here as we are setting
                // additional parameters that may not be
                // required by the mapper. If they are indeed required, the
                // configure method invoked on the mapper
                // shall report an exception because of the missing parameters.
            }
        }

        protected void initializeMapper() throws HyracksDataException {
            super.initializeMapper();
            updateConfWithSplit(conf);
            try {
                mapper = createMapper(conf);
            } catch (Exception e) {
                throw new HyracksDataException(e);
            }
            if (!conf.getUseNewMapper()) {
                ((org.apache.hadoop.mapred.Mapper) mapper).configure(conf);
            }
        }

        public void mapInput() throws HyracksDataException, InterruptedException, ClassNotFoundException {
            try {
                initializeMapper();
                conf.setClassLoader(this.getClass().getClassLoader());
                Object reader;
                Object key = null;
                Object value = null;
                Object inputSplit = inputSplits[partition];
                reader = getRecordReader(conf, inputSplit);
                final Object[] data = new Object[2];
                if (conf.getUseNewMapper()) {
                    org.apache.hadoop.mapreduce.RecordReader newReader = (org.apache.hadoop.mapreduce.RecordReader) reader;
                    org.apache.hadoop.mapreduce.RecordWriter recordWriter = new RecordWriter() {

                        @Override
                        public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
                            // TODO Auto-generated method stub
                        }

                        @Override
                        public void write(Object key, Object value) throws IOException, InterruptedException {
                            data[0] = key;
                            data[1] = value;
                            writer.writeData(data);
                        }
                    };;;

                    OutputCommitter outputCommitter = new org.apache.hadoop.mapreduce.lib.output.NullOutputFormat()
                            .getOutputCommitter(new TaskAttemptContext(conf, new TaskAttemptID()));
                    StatusReporter statusReporter = new StatusReporter() {
                        @Override
                        public void setStatus(String arg0) {
                        }

                        @Override
                        public void progress() {
                        }

                        @Override
                        public Counter getCounter(String arg0, String arg1) {
                            return null;
                        }

                        @Override
                        public Counter getCounter(Enum<?> arg0) {
                            return null;
                        }
                    };;;
                    context = new org.apache.hadoop.mapreduce.Mapper().new Context(conf, new TaskAttemptID(),
                            newReader, recordWriter, outputCommitter, statusReporter,
                            (org.apache.hadoop.mapreduce.InputSplit) inputSplit);
                    newReader.initialize((org.apache.hadoop.mapreduce.InputSplit) inputSplit, context);
                    ((org.apache.hadoop.mapreduce.Mapper) mapper).run(context);
                } else {
                    Class inputKeyClass = null;
                    Class inputValueClass = null;

                    RecordReader oldReader = (RecordReader) reader;
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
    private Class mapperClass;
    private InputSplitsProxy inputSplitsProxy;
    private transient Object[] inputSplits;
    private boolean selfRead = false;

    private void initializeSplitInfo(Object[] splits) throws IOException {
        jobConf = super.getJobConf();
        InputFormat inputFormat = jobConf.getInputFormat();
        inputSplitsProxy = new InputSplitsProxy(jobConf, splits);
    }

    public HadoopMapperOperatorDescriptor(IOperatorDescriptorRegistry spec, JobConf jobConf,
            IHadoopClassFactory hadoopClassFactory) throws IOException {
        super(spec, 1, getRecordDescriptor(jobConf, hadoopClassFactory), jobConf, hadoopClassFactory);
    }

    public HadoopMapperOperatorDescriptor(IOperatorDescriptorRegistry spec, JobConf jobConf, Object[] splits,
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
                recordDescriptor = DatatypeHelper.createKeyValueRecordDescriptor(
                        (Class<? extends Writable>) Class.forName(mapOutputKeyClassName),
                        (Class<? extends Writable>) Class.forName(mapOutputValueClassName));
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

    private Object createMapper(JobConf conf) throws Exception {
        Object mapper;
        if (mapperClass != null) {
            return ReflectionUtils.newInstance(mapperClass, conf);
        } else {
            String mapperClassName = null;
            if (jobConf.getUseNewMapper()) {
                JobContext jobContext = new JobContext(conf, null);
                mapperClass = jobContext.getMapperClass();
                mapperClassName = mapperClass.getName();
            } else {
                mapperClass = conf.getMapperClass();
                mapperClassName = mapperClass.getName();
            }
            mapper = getHadoopClassFactory().createMapper(mapperClassName, conf);
        }
        return mapper;
    }

    private Object getRecordReader(JobConf conf, Object inputSplit) throws ClassNotFoundException, IOException,
            InterruptedException {
        if (conf.getUseNewMapper()) {
            JobContext context = new JobContext(conf, null);
            org.apache.hadoop.mapreduce.InputFormat inputFormat = (org.apache.hadoop.mapreduce.InputFormat) ReflectionUtils
                    .newInstance(context.getInputFormatClass(), conf);
            TaskAttemptContext taskAttemptContext = new org.apache.hadoop.mapreduce.TaskAttemptContext(conf,
                    new TaskAttemptID());
            return inputFormat.createRecordReader((org.apache.hadoop.mapreduce.InputSplit) inputSplit,
                    taskAttemptContext);
        } else {
            Class inputFormatClass = conf.getInputFormat().getClass();
            InputFormat inputFormat = (InputFormat) ReflectionUtils.newInstance(inputFormatClass, conf);
            return inputFormat.getRecordReader((org.apache.hadoop.mapred.InputSplit) inputSplit, conf,
                    super.createReporter());
        }
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {

        JobConf conf = getJobConf();
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
        try {
            if (selfRead) {
                RecordDescriptor recordDescriptor = null;
                if (inputSplits == null) {
                    inputSplits = inputSplitsProxy.toInputSplits(conf);
                }
                Object reader = getRecordReader(conf, inputSplits[partition]);
                if (conf.getUseNewMapper()) {
                    org.apache.hadoop.mapreduce.RecordReader newReader = (org.apache.hadoop.mapreduce.RecordReader) reader;
                    newReader.initialize((org.apache.hadoop.mapreduce.InputSplit) inputSplits[partition],
                            new TaskAttemptContext(conf, new TaskAttemptID()));
                    newReader.nextKeyValue();
                    Object key = newReader.getCurrentKey();
                    Class keyClass = null;
                    if (key == null) {
                        keyClass = Class.forName("org.apache.hadoop.io.NullWritable");
                    }
                    recordDescriptor = DatatypeHelper.createKeyValueRecordDescriptor(
                            (Class<? extends Writable>) keyClass, (Class<? extends Writable>) newReader
                                    .getCurrentValue().getClass());
                } else {
                    RecordReader oldReader = (RecordReader) reader;
                    recordDescriptor = DatatypeHelper.createKeyValueRecordDescriptor(
                            (Class<? extends Writable>) oldReader.createKey().getClass(),
                            (Class<? extends Writable>) oldReader.createValue().getClass());
                }
                return createSelfReadingMapper(ctx, recordDescriptor, partition);
            } else {
                return new DeserializedOperatorNodePushable(ctx, new MapperOperator(partition),
                        recordDescProvider.getInputRecordDescriptor(this.activityNodeId, 0));
            }
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    private IOperatorNodePushable createSelfReadingMapper(final IHyracksTaskContext ctx,
            final RecordDescriptor recordDescriptor, final int partition) {
        return new AbstractUnaryOutputSourceOperatorNodePushable() {
            @Override
            public void initialize() throws HyracksDataException {
                SerializingDataWriter writer = new SerializingDataWriter(ctx, recordDescriptor, this.writer);
                ReaderMapperOperator readMapOp = new ReaderMapperOperator(partition, writer);
                try {
                    readMapOp.mapInput();
                } catch (Exception e) {
                    writer.fail();
                    throw new HyracksDataException(e);
                } finally {
                    readMapOp.close();
                }
            }
        };
    }

    public Class<? extends Mapper> getMapperClass() {
        return mapperClass;
    }
}
