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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.dataflow.IOpenableDataWriter;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.hadoop.util.DatatypeHelper;
import edu.uci.ics.hyracks.dataflow.hadoop.util.IHadoopClassFactory;
import edu.uci.ics.hyracks.dataflow.hadoop.util.InputSplitsProxy;
import edu.uci.ics.hyracks.dataflow.std.base.IOpenableDataWriterOperator;
import edu.uci.ics.hyracks.dataflow.std.util.DeserializedOperatorNodePushable;

public class HadoopMapperOperatorDescriptor<K1, V1, K2, V2> extends AbstractHadoopOperatorDescriptor {
    private class MapperOperator implements IOpenableDataWriterOperator {
        private OutputCollector<K2, V2> output;
        private Reporter reporter;
        private Mapper<K1, V1, K2, V2> mapper;
        private IOpenableDataWriter<Object[]> writer;
        private int partition;

        public MapperOperator(int partition) {
            this.partition = partition;
        };

        @Override
        public void close() throws HyracksDataException {
            try {
                mapper.close();
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
            writer.close();
        }

        @Override
        public void open() throws HyracksDataException {
            jobConf = getJobConf();
            populateCache(jobConf);
            Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
            try {
                mapper = createMapper();
            } catch (Exception e) {
                throw new HyracksDataException(e);
            }
            if (inputSplitsProxy != null) {
                updateConfWithSplit();
            }
            mapper.configure(jobConf);
            writer.open();
            output = new DataWritingOutputCollector<K2, V2>(writer);
            reporter = createReporter();
        }

        private void updateConfWithSplit() {
            try {
                InputSplit[] splits = inputSplitsProxy.toInputSplits(jobConf);
                InputSplit splitRead = splits[partition];
                if (splitRead instanceof FileSplit) {
                    jobConf.set("map.input.file", ((FileSplit) splitRead).getPath().toString());
                    jobConf.setLong("map.input.start", ((FileSplit) splitRead).getStart());
                    jobConf.setLong("map.input.length", ((FileSplit) splitRead).getLength());
                }
            } catch (Exception e) {
                e.printStackTrace();
                // we do not throw the exception here as we are setting additional parameters that may not be 
                // required by the mapper. If they are  indeed required,  the configure method invoked on the mapper
                // shall report an exception because of the missing parameters. 
            }
        }

        @Override
        public void setDataWriter(int index, IOpenableDataWriter<Object[]> writer) {
            if (index != 0) {
                throw new IllegalArgumentException();
            }
            this.writer = writer;
        }

        @Override
        public void writeData(Object[] data) throws HyracksDataException {
            try {
                mapper.map((K1) data[0], (V1) data[1], output, reporter);
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }
    }

    private static final long serialVersionUID = 1L;
    private Class<? extends Mapper> mapperClass;
    private InputSplitsProxy inputSplitsProxy;
    private transient InputSplit[] inputSplits;

    private void initializeSplitInfo(InputSplit[] splits) throws IOException {
        jobConf = super.getJobConf();
        InputFormat inputFormat = jobConf.getInputFormat();
        inputSplitsProxy = new InputSplitsProxy(splits);
    }

    public HadoopMapperOperatorDescriptor(JobSpecification spec, JobConf jobConf, InputSplit[] splits,
            IHadoopClassFactory hadoopClassFactory) throws IOException {
        super(spec, getRecordDescriptor(jobConf, hadoopClassFactory), jobConf, hadoopClassFactory);
        if (splits != null) {
            initializeSplitInfo(splits);
        }
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

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksContext ctx, IOperatorEnvironment env,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
        RecordDescriptor recordDescriptor = null;
        JobConf conf = getJobConf();
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
        try {
            if (inputSplits == null) {
                inputSplits = inputSplitsProxy.toInputSplits(conf);
            }
            RecordReader reader = conf.getInputFormat().getRecordReader(inputSplits[partition], conf,
                    super.createReporter());
            recordDescriptor = DatatypeHelper.createKeyValueRecordDescriptor((Class<? extends Writable>) reader
                    .createKey().getClass(), (Class<? extends Writable>) reader.createValue().getClass());
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }

        return new DeserializedOperatorNodePushable(ctx, new MapperOperator(partition), recordDescriptor);
    }

    public Class<? extends Mapper> getMapperClass() {
        return mapperClass;
    }
}
