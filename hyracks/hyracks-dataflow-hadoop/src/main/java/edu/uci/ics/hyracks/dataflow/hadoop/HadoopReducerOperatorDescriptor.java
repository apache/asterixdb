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
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.ReflectionUtils;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IDataReader;
import edu.uci.ics.hyracks.api.dataflow.IDataWriter;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.hadoop.data.KeyComparatorFactory;
import edu.uci.ics.hyracks.dataflow.hadoop.data.RawComparingComparatorFactory;
import edu.uci.ics.hyracks.dataflow.hadoop.util.DatatypeHelper;
import edu.uci.ics.hyracks.dataflow.hadoop.util.IHadoopClassFactory;
import edu.uci.ics.hyracks.dataflow.std.base.IOpenableDataWriterOperator;
import edu.uci.ics.hyracks.dataflow.std.group.DeserializedPreclusteredGroupOperator;
import edu.uci.ics.hyracks.dataflow.std.group.IGroupAggregator;
import edu.uci.ics.hyracks.dataflow.std.util.DeserializedOperatorNodePushable;

public class HadoopReducerOperatorDescriptor<K2, V2, K3, V3> extends AbstractHadoopOperatorDescriptor {
    private class ReducerAggregator implements IGroupAggregator {
        private Object reducer;
        private DataWritingOutputCollector<K3, V3> output;
        private Reporter reporter;
        private ReducerContext reducerContext;
        RawKeyValueIterator rawKeyValueIterator = new RawKeyValueIterator() {

            @Override
            public boolean next() throws IOException {
                return false;
            }

            @Override
            public DataInputBuffer getValue() throws IOException {
                return null;
            }

            @Override
            public Progress getProgress() {
                return null;
            }

            @Override
            public DataInputBuffer getKey() throws IOException {
                return null;
            }

            @Override
            public void close() throws IOException {

            }
        };

        class ReducerContext extends org.apache.hadoop.mapreduce.Reducer.Context {
            private HadoopReducerOperatorDescriptor.ValueIterator iterator;

            @SuppressWarnings("unchecked")
            ReducerContext(org.apache.hadoop.mapreduce.Reducer reducer, JobConf conf) throws IOException,
                    InterruptedException, ClassNotFoundException {

                reducer.super(conf, new TaskAttemptID(), rawKeyValueIterator, null, null, null, null, null, null, Class
                        .forName("org.apache.hadoop.io.NullWritable"), Class
                        .forName("org.apache.hadoop.io.NullWritable"));
            }

            public void setIterator(HadoopReducerOperatorDescriptor.ValueIterator iter) {
                iterator = iter;
            }

            @Override
            public Iterable<V2> getValues() throws IOException, InterruptedException {
                return new Iterable<V2>() {
                    @Override
                    public Iterator<V2> iterator() {
                        return iterator;
                    }
                };
            }

            /** Start processing next unique key. */
            @Override
            public boolean nextKey() throws IOException, InterruptedException {
                boolean hasMore = iterator.hasNext();
                if (hasMore) {
                    nextKeyValue();
                }
                return hasMore;
            }

            /**
             * Advance to the next key/value pair.
             */
            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {
                iterator.next();
                return true;
            }

            public Object getCurrentKey() {
                return iterator.getKey();
            }

            @Override
            public Object getCurrentValue() {
                return iterator.getValue();
            }

            /**
             * Generate an output key/value pair.
             */
            @Override
            public void write(Object key, Object value) throws IOException, InterruptedException {
                output.collect(key, value);
            }

        }

        public ReducerAggregator(Object reducer) throws HyracksDataException {
            this.reducer = reducer;
            initializeReducer();
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
            Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
            ValueIterator i = new ValueIterator();
            i.reset(reader);
            output.setWriter(writer);
            try {
                if (jobConf.getUseNewReducer()) {
                    try {
                        reducerContext.setIterator(i);
                        ((org.apache.hadoop.mapreduce.Reducer) reducer).run(reducerContext);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        throw new HyracksDataException(e);
                    }
                } else {
                    ((org.apache.hadoop.mapred.Reducer) reducer).reduce(i.getKey(), i, output, reporter);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void close() throws HyracksDataException {
            // -- - close - --
            try {
                if (!jobConf.getUseNewMapper()) {
                    ((org.apache.hadoop.mapred.Reducer) reducer).close();
                }
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }

        private void initializeReducer() throws HyracksDataException {
            jobConf.setClassLoader(this.getClass().getClassLoader());
            if (!jobConf.getUseNewReducer()) {
                ((org.apache.hadoop.mapred.Reducer) reducer).configure(getJobConf());
            } else {
                try {
                    reducerContext = new ReducerContext((org.apache.hadoop.mapreduce.Reducer) reducer, jobConf);
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new HyracksDataException(e);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new HyracksDataException(e);
                } catch (RuntimeException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
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

        public V2 getValue() {
            return value;
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
    private Class reducerClass;
    private IComparatorFactory comparatorFactory;
    private boolean useAsCombiner = false;

    public HadoopReducerOperatorDescriptor(IOperatorDescriptorRegistry spec, JobConf conf,
            IComparatorFactory comparatorFactory, IHadoopClassFactory classFactory, boolean useAsCombiner) {
        super(spec, 1, getRecordDescriptor(conf, classFactory), conf, classFactory);
        this.comparatorFactory = comparatorFactory;
        this.useAsCombiner = useAsCombiner;
    }

    private Object createReducer() throws Exception {
        if (reducerClass != null) {
            return ReflectionUtils.newInstance(reducerClass, getJobConf());
        } else {
            Object reducer;
            if (!useAsCombiner) {
                if (getJobConf().getUseNewReducer()) {
                    JobContext jobContext = new JobContext(getJobConf(), null);
                    reducerClass = (Class<? extends org.apache.hadoop.mapreduce.Reducer<?, ?, ?, ?>>) jobContext
                            .getReducerClass();
                } else {
                    reducerClass = (Class<? extends Reducer>) getJobConf().getReducerClass();
                }
            } else {
                if (getJobConf().getUseNewReducer()) {
                    JobContext jobContext = new JobContext(getJobConf(), null);
                    reducerClass = (Class<? extends org.apache.hadoop.mapreduce.Reducer<?, ?, ?, ?>>) jobContext
                            .getCombinerClass();
                } else {
                    reducerClass = (Class<? extends Reducer>) getJobConf().getCombinerClass();
                }
            }
            reducer = getHadoopClassFactory().createReducer(reducerClass.getName(), getJobConf());
            return reducer;
        }
    }

    @Override
    public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) {
        try {
            if (this.comparatorFactory == null) {
                String comparatorClassName = getJobConf().getOutputValueGroupingComparator().getClass().getName();
                Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
                RawComparator rawComparator = null;
                if (comparatorClassName != null) {
                    Class comparatorClazz = getHadoopClassFactory().loadClass(comparatorClassName);
                    this.comparatorFactory = new KeyComparatorFactory(comparatorClazz);

                } else {
                    String mapOutputKeyClass = getJobConf().getMapOutputKeyClass().getName();
                    if (getHadoopClassFactory() != null) {
                        rawComparator = WritableComparator.get(getHadoopClassFactory().loadClass(mapOutputKeyClass));
                    } else {
                        rawComparator = WritableComparator.get((Class<? extends WritableComparable>) Class
                                .forName(mapOutputKeyClass));
                    }
                    this.comparatorFactory = new RawComparingComparatorFactory(rawComparator.getClass());
                }
            }
            IOpenableDataWriterOperator op = new DeserializedPreclusteredGroupOperator(new int[] { 0 },
                    new IComparator[] { comparatorFactory.createComparator() }, new ReducerAggregator(createReducer()));
            return new DeserializedOperatorNodePushable(ctx, op, recordDescProvider.getInputRecordDescriptor(
                    getActivityId(), 0));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static RecordDescriptor getRecordDescriptor(JobConf conf, IHadoopClassFactory classFactory) {
        String outputKeyClassName = null;
        String outputValueClassName = null;

        if (conf.getUseNewMapper()) {
            JobContext context = new JobContext(conf, null);
            outputKeyClassName = context.getOutputKeyClass().getName();
            outputValueClassName = context.getOutputValueClass().getName();
        } else {
            outputKeyClassName = conf.getOutputKeyClass().getName();
            outputValueClassName = conf.getOutputValueClass().getName();
        }

        RecordDescriptor recordDescriptor = null;
        try {
            if (classFactory == null) {
                recordDescriptor = DatatypeHelper.createKeyValueRecordDescriptor(
                        (Class<? extends Writable>) Class.forName(outputKeyClassName),
                        (Class<? extends Writable>) Class.forName(outputValueClassName));
            } else {
                recordDescriptor = DatatypeHelper.createKeyValueRecordDescriptor(
                        (Class<? extends Writable>) classFactory.loadClass(outputKeyClassName),
                        (Class<? extends Writable>) classFactory.loadClass(outputValueClassName));
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return recordDescriptor;
    }
}
