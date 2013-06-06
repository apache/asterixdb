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
package edu.uci.ics.hyracks.dataflow.hadoop.mapreduce;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.ReflectionUtils;

import edu.uci.ics.hyracks.api.context.IHyracksCommonContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.hadoop.data.HadoopNewPartitionerTuplePartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.hadoop.data.WritableComparingBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.hadoop.util.DatatypeHelper;

public class HadoopHelper {
    public static final int KEY_FIELD_INDEX = 0;
    public static final int VALUE_FIELD_INDEX = 1;
    public static final int BLOCKID_FIELD_INDEX = 2;
    private static final int[] KEY_SORT_FIELDS = new int[] { 0 };

    private MarshalledWritable<Configuration> mConfig;
    private Configuration config;
    private Job job;

    public HadoopHelper(MarshalledWritable<Configuration> mConfig) throws HyracksDataException {
        this.mConfig = mConfig;
        ClassLoader ctxCL = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            config = mConfig.get();
            config.setClassLoader(getClass().getClassLoader());
            job = new Job(config);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        } finally {
            Thread.currentThread().setContextClassLoader(ctxCL);
        }
    }

    public RecordDescriptor getMapOutputRecordDescriptor() throws HyracksDataException {
        try {
            return new RecordDescriptor(
                    new ISerializerDeserializer[] {
                            DatatypeHelper.createSerializerDeserializer((Class<? extends Writable>) job
                                    .getMapOutputKeyClass()),
                            DatatypeHelper.createSerializerDeserializer((Class<? extends Writable>) job
                                    .getMapOutputValueClass()), IntegerSerializerDeserializer.INSTANCE });

        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    public RecordDescriptor getMapOutputRecordDescriptorWithoutExtraFields() throws HyracksDataException {
        try {
            return new RecordDescriptor(
                    new ISerializerDeserializer[] {
                            DatatypeHelper.createSerializerDeserializer((Class<? extends Writable>) job
                                    .getMapOutputKeyClass()),
                            DatatypeHelper.createSerializerDeserializer((Class<? extends Writable>) job
                                    .getMapOutputValueClass()) });

        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    public TaskAttemptContext createTaskAttemptContext(TaskAttemptID taId) {
        ClassLoader ctxCL = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(config.getClassLoader());
            return new TaskAttemptContext(config, taId);
        } finally {
            Thread.currentThread().setContextClassLoader(ctxCL);
        }
    }

    public JobContext createJobContext() {
        ClassLoader ctxCL = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(config.getClassLoader());
            return new JobContext(config, null);
        } finally {
            Thread.currentThread().setContextClassLoader(ctxCL);
        }
    }

    public <K1, V1, K2, V2> Mapper<K1, V1, K2, V2> getMapper() throws HyracksDataException {
        try {
            return (Mapper<K1, V1, K2, V2>) HadoopTools.newInstance(job.getMapperClass());
        } catch (ClassNotFoundException e) {
            throw new HyracksDataException(e);
        } catch (InstantiationException e) {
            throw new HyracksDataException(e);
        } catch (IllegalAccessException e) {
            throw new HyracksDataException(e);
        }
    }

    public <K2, V2, K3, V3> Reducer<K2, V2, K3, V3> getReducer() throws HyracksDataException {
        try {
            return (Reducer<K2, V2, K3, V3>) HadoopTools.newInstance(job.getReducerClass());
        } catch (ClassNotFoundException e) {
            throw new HyracksDataException(e);
        } catch (InstantiationException e) {
            throw new HyracksDataException(e);
        } catch (IllegalAccessException e) {
            throw new HyracksDataException(e);
        }
    }

    public <K2, V2> Reducer<K2, V2, K2, V2> getCombiner() throws HyracksDataException {
        try {
            return (Reducer<K2, V2, K2, V2>) HadoopTools.newInstance(job.getCombinerClass());
        } catch (ClassNotFoundException e) {
            throw new HyracksDataException(e);
        } catch (InstantiationException e) {
            throw new HyracksDataException(e);
        } catch (IllegalAccessException e) {
            throw new HyracksDataException(e);
        }
    }

    public <K, V> InputFormat<K, V> getInputFormat() throws HyracksDataException {
        try {
            return (InputFormat<K, V>) ReflectionUtils.newInstance(job.getInputFormatClass(), config);
        } catch (ClassNotFoundException e) {
            throw new HyracksDataException(e);
        }
    }

    public <K, V> List<InputSplit> getInputSplits() throws HyracksDataException {
        ClassLoader ctxCL = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            InputFormat<K, V> fmt = getInputFormat();
            JobContext jCtx = new JobContext(config, null);
            try {
                return fmt.getSplits(jCtx);
            } catch (IOException e) {
                throw new HyracksDataException(e);
            } catch (InterruptedException e) {
                throw new HyracksDataException(e);
            }
        } finally {
            Thread.currentThread().setContextClassLoader(ctxCL);
        }
    }

    public IBinaryComparatorFactory[] getSortComparatorFactories() {
        WritableComparingBinaryComparatorFactory comparatorFactory = new WritableComparingBinaryComparatorFactory(job
                .getSortComparator().getClass());

        return new IBinaryComparatorFactory[] { comparatorFactory };
    }

    public IBinaryComparatorFactory[] getGroupingComparatorFactories() {
        WritableComparingBinaryComparatorFactory comparatorFactory = new WritableComparingBinaryComparatorFactory(job
                .getGroupingComparator().getClass());

        return new IBinaryComparatorFactory[] { comparatorFactory };
    }

    public RawComparator<?> getRawGroupingComparator() {
        return job.getGroupingComparator();
    }

    public int getSortFrameLimit(IHyracksCommonContext ctx) {
        int sortMemory = job.getConfiguration().getInt("io.sort.mb", 100);
        return (int) (((long) sortMemory * 1024 * 1024) / ctx.getFrameSize());
    }

    public Job getJob() {
        return job;
    }

    public MarshalledWritable<Configuration> getMarshalledConfiguration() {
        return mConfig;
    }

    public Configuration getConfiguration() {
        return config;
    }

    public ITuplePartitionComputerFactory getTuplePartitionComputer() throws HyracksDataException {
        int nReducers = job.getNumReduceTasks();
        try {
            return new HadoopNewPartitionerTuplePartitionComputerFactory<Writable, Writable>(
                    (Class<? extends Partitioner<Writable, Writable>>) job.getPartitionerClass(),
                    (ISerializerDeserializer<Writable>) DatatypeHelper
                            .createSerializerDeserializer((Class<? extends Writable>) job.getMapOutputKeyClass()),
                    (ISerializerDeserializer<Writable>) DatatypeHelper
                            .createSerializerDeserializer((Class<? extends Writable>) job.getMapOutputValueClass()));
        } catch (ClassNotFoundException e) {
            throw new HyracksDataException(e);
        }
    }

    public int[] getSortFields() {
        return KEY_SORT_FIELDS;
    }

    public <K> ISerializerDeserializer<K> getMapOutputKeySerializerDeserializer() {
        return (ISerializerDeserializer<K>) DatatypeHelper.createSerializerDeserializer((Class<? extends Writable>) job
                .getMapOutputKeyClass());
    }

    public <V> ISerializerDeserializer<V> getMapOutputValueSerializerDeserializer() {
        return (ISerializerDeserializer<V>) DatatypeHelper.createSerializerDeserializer((Class<? extends Writable>) job
                .getMapOutputValueClass());
    }

    public FileSystem getFilesystem() throws HyracksDataException {
        try {
            return FileSystem.get(config);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    public <K, V> OutputFormat<K, V> getOutputFormat() throws HyracksDataException {
        try {
            return (OutputFormat<K, V>) ReflectionUtils.newInstance(job.getOutputFormatClass(), config);
        } catch (ClassNotFoundException e) {
            throw new HyracksDataException(e);
        }
    }

    public boolean hasCombiner() throws HyracksDataException {
        try {
            return job.getCombinerClass() != null;
        } catch (ClassNotFoundException e) {
            throw new HyracksDataException(e);
        }
    }
}