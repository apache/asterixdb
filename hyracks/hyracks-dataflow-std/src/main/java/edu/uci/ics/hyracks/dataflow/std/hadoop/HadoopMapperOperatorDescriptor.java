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
package edu.uci.ics.hyracks.dataflow.std.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.dataflow.IOpenableDataWriter;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.IOpenableDataWriterOperator;
import edu.uci.ics.hyracks.dataflow.std.hadoop.util.DatatypeHelper;
import edu.uci.ics.hyracks.dataflow.std.hadoop.util.IHadoopClassFactory;
import edu.uci.ics.hyracks.dataflow.std.util.DeserializedOperatorNodePushable;

public class HadoopMapperOperatorDescriptor<K1, V1, K2, V2> extends AbstractHadoopOperatorDescriptor {
    private class MapperOperator implements IOpenableDataWriterOperator {
        private OutputCollector<K2, V2> output;
        private Reporter reporter;
        private Mapper<K1, V1, K2, V2> mapper;
        private IOpenableDataWriter<Object[]> writer;

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
            JobConf jobConf = getJobConf();
            populateCache(jobConf);
            try {
                mapper = createMapper();
            } catch (Exception e) {
                throw new HyracksDataException(e);
            }
            // -- - configure - --
            mapper.configure(jobConf);
            writer.open();
            output = new DataWritingOutputCollector<K2, V2>(writer);
            reporter = createReporter();
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
    private static final String mapClassNameKey = "mapred.mapper.class";
    private Class<? extends Mapper> mapperClass;

    public HadoopMapperOperatorDescriptor(JobSpecification spec, Class<? extends Mapper> mapperClass,
            RecordDescriptor recordDescriptor, JobConf jobConf) {
        super(spec, recordDescriptor, jobConf, null);
        this.mapperClass = mapperClass;
    }

    public HadoopMapperOperatorDescriptor(JobSpecification spec, JobConf jobConf, IHadoopClassFactory hadoopClassFactory) {
        super(spec, null, jobConf, hadoopClassFactory);
    }

    public RecordDescriptor getRecordDescriptor(JobConf conf) {
        RecordDescriptor recordDescriptor = null;
        String mapOutputKeyClassName = conf.getMapOutputKeyClass().getName();
        String mapOutputValueClassName = conf.getMapOutputValueClass().getName();
        try {
            if (getHadoopClassFactory() == null) {
                recordDescriptor = DatatypeHelper.createKeyValueRecordDescriptor(
                        (Class<? extends Writable>) Class.forName(mapOutputKeyClassName),
                        (Class<? extends Writable>) Class.forName(mapOutputValueClassName));
            } else {
                recordDescriptor = DatatypeHelper.createKeyValueRecordDescriptor(
                        (Class<? extends Writable>) getHadoopClassFactory().loadClass(mapOutputKeyClassName),
                        (Class<? extends Writable>) getHadoopClassFactory().loadClass(mapOutputValueClassName));
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
            String mapperClassName = super.getJobConfMap().get(mapClassNameKey);
            Object mapper = getHadoopClassFactory().createMapper(mapperClassName);
            mapperClass = (Class<? extends Mapper>) mapper.getClass();
            return (Mapper) mapper;
        }
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksContext ctx, IOperatorEnvironment env,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        return new DeserializedOperatorNodePushable(ctx, new MapperOperator(),
                recordDescProvider.getInputRecordDescriptor(getOperatorId(), 0));
    }

    public Class<? extends Mapper> getMapperClass() {
        return mapperClass;
    }
}
