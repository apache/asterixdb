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
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.dataflow.IDataReader;
import edu.uci.ics.hyracks.api.dataflow.IDataWriter;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.IOpenableDataWriterOperator;
import edu.uci.ics.hyracks.dataflow.std.group.IGroupAggregator;
import edu.uci.ics.hyracks.dataflow.std.group.PreclusteredGroupOperator;
import edu.uci.ics.hyracks.dataflow.std.hadoop.data.KeyComparatorFactory;
import edu.uci.ics.hyracks.dataflow.std.hadoop.data.WritableComparingComparatorFactory;
import edu.uci.ics.hyracks.dataflow.std.hadoop.util.ClasspathBasedHadoopClassFactory;
import edu.uci.ics.hyracks.dataflow.std.hadoop.util.DatatypeHelper;
import edu.uci.ics.hyracks.dataflow.std.hadoop.util.IHadoopClassFactory;
import edu.uci.ics.hyracks.dataflow.std.util.DeserializedOperatorNodePushable;

public class HadoopReducerOperatorDescriptor<K2, V2, K3, V3> extends AbstractHadoopOperatorDescriptor {
    private class ReducerAggregator implements IGroupAggregator {
        private Reducer<K2, V2, K3, V3> reducer;
        private DataWritingOutputCollector<K3, V3> output;
        private Reporter reporter;

        public ReducerAggregator(Reducer<K2, V2, K3, V3> reducer) {
            this.reducer = reducer;
            reducer.configure(getJobConf());
            output = new DataWritingOutputCollector<K3, V3>();
            reporter = new Reporter() {

                @Override
                public void progress() {

                }

                @Override
                public void setStatus(String arg0) {

                }

                @Override
                public void incrCounter(String arg0, String arg1, long arg2) {

                }

                @Override
                public void incrCounter(Enum<?> arg0, long arg1) {

                }

                @Override
                public InputSplit getInputSplit() throws UnsupportedOperationException {
                    return null;
                }

                @Override
                public Counter getCounter(String arg0, String arg1) {
                    return null;
                }

                @Override
                public Counter getCounter(Enum<?> arg0) {
                    return null;
                }
            };
        }

        @Override
        public void aggregate(IDataReader<Object[]> reader, IDataWriter<Object[]> writer) throws HyracksDataException {

            ValueIterator i = new ValueIterator();
            i.reset(reader);
            output.setWriter(writer);
            try {

                // -- - reduce - --
                reducer.reduce(i.getKey(), i, output, reporter);

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void close() throws HyracksDataException {
            // -- - close - --
            try {
                reducer.close();
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }
    }

    private class ValueIterator implements Iterator<V2> {
        private IDataReader<Object[]> reader;
        private K2 key;
        private V2 value;

        public K2 getKey() {
            return key;
        }

        @Override
        public boolean hasNext() {
            if (value == null) {
                Object[] tuple;
                try {
                    tuple = reader.readData();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                if (tuple != null) {
                    value = (V2) tuple[1];
                }
            }
            return value != null;
        }

        @Override
        public V2 next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            V2 v = value;
            value = null;
            return v;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        void reset(IDataReader<Object[]> reader) {
            this.reader = reader;
            try {
                Object[] tuple = reader.readData();
                key = (K2) tuple[0];
                value = (V2) tuple[1];
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static final long serialVersionUID = 1L;
    private Class<? extends Reducer> reducerClass;
    private static final String reducerClassKey = "mapred.reducer.class";
    private static final String comparatorClassKey = "mapred.output.value.groupfn.class";
    private IComparatorFactory comparatorFactory;

    public HadoopReducerOperatorDescriptor(JobSpecification spec, IComparatorFactory comparatorFactory,
            Class<? extends Reducer> reducerClass, RecordDescriptor recordDescriptor, JobConf jobConf) {
        super(spec, recordDescriptor, jobConf, new ClasspathBasedHadoopClassFactory());
        this.comparatorFactory = comparatorFactory;
        this.reducerClass = reducerClass;
    }

    public HadoopReducerOperatorDescriptor(JobSpecification spec, JobConf conf, IHadoopClassFactory classFactory) {
        super(spec, null, conf, classFactory);
    }

    private Reducer<K2, V2, K3, V3> createReducer() throws Exception {
        if (reducerClass != null) {
            return reducerClass.newInstance();
        } else {
            Object reducer = getHadoopClassFactory().createReducer(getJobConfMap().get(reducerClassKey));
            reducerClass = (Class<? extends Reducer>) reducer.getClass();
            return (Reducer) reducer;
        }
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksContext ctx, IOperatorEnvironment env,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        try {
            if (this.comparatorFactory == null) {
                String comparatorClassName = getJobConfMap().get(comparatorClassKey);
                RawComparator rawComparator = null;
                if (comparatorClassName != null) {
                    Class comparatorClazz = getHadoopClassFactory().loadClass(comparatorClassName);
                    this.comparatorFactory = new KeyComparatorFactory(comparatorClazz);

                } else {
                    String mapOutputKeyClass = getJobConfMap().get("mapred.mapoutput.key.class");
                    if (getHadoopClassFactory() != null) {
                        rawComparator = WritableComparator.get(getHadoopClassFactory().loadClass(mapOutputKeyClass));
                    } else {
                        rawComparator = WritableComparator.get((Class<? extends WritableComparable>) Class
                                .forName(mapOutputKeyClass));
                    }
                    this.comparatorFactory = new WritableComparingComparatorFactory(rawComparator.getClass());
                }
            }
            IOpenableDataWriterOperator op = new PreclusteredGroupOperator(new int[] { 0 },
                    new IComparator[] { comparatorFactory.createComparator() }, new ReducerAggregator(createReducer()));
            return new DeserializedOperatorNodePushable(ctx, op, recordDescProvider.getInputRecordDescriptor(
                    getOperatorId(), 0));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Class<? extends Reducer> getReducerClass() {
        return reducerClass;
    }

    public void setReducerClass(Class<? extends Reducer> reducerClass) {
        this.reducerClass = reducerClass;
    }

    @Override
    public RecordDescriptor getRecordDescriptor(JobConf conf) {
        String outputKeyClassName = conf.get("mapred.output.key.class");
        String outputValueClassName = conf.get("mapred.output.value.class");
        RecordDescriptor recordDescriptor = null;
        try {
            if (getHadoopClassFactory() == null) {
                recordDescriptor = DatatypeHelper.createKeyValueRecordDescriptor(
                        (Class<? extends Writable>) Class.forName(outputKeyClassName),
                        (Class<? extends Writable>) Class.forName(outputValueClassName));
            } else {
                recordDescriptor = DatatypeHelper.createKeyValueRecordDescriptor(
                        (Class<? extends Writable>) getHadoopClassFactory().loadClass(outputKeyClassName),
                        (Class<? extends Writable>) getHadoopClassFactory().loadClass(outputValueClassName));
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return recordDescriptor;
    }
}
