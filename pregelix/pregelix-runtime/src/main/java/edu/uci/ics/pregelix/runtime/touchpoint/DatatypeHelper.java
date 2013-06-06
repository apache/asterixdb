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
package edu.uci.ics.pregelix.runtime.touchpoint;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

@SuppressWarnings("deprecation")
public class DatatypeHelper {
    private static final class WritableSerializerDeserializer<T extends Writable> implements ISerializerDeserializer<T> {
        private static final long serialVersionUID = 1L;

        private Class<T> clazz;
        private T object;

        private WritableSerializerDeserializer(Class<T> clazz) {
            this.clazz = clazz;
        }

        @SuppressWarnings("unchecked")
        private T createInstance() throws HyracksDataException {
            // TODO remove "if", create a new WritableInstanceOperations class
            // that deals with Writables that don't have public constructors
            if (NullWritable.class.equals(clazz)) {
                return (T) NullWritable.get();
            }
            try {
                return clazz.newInstance();
            } catch (InstantiationException e) {
                throw new HyracksDataException(e);
            } catch (IllegalAccessException e) {
                throw new HyracksDataException(e);
            }
        }

        @Override
        public T deserialize(DataInput in) throws HyracksDataException {
            if (object == null) {
                object = createInstance();
            }
            try {
                object.readFields(in);
            } catch (IOException e) {
                e.printStackTrace();
                throw new HyracksDataException(e);
            }
            return object;
        }

        @Override
        public void serialize(T instance, DataOutput out) throws HyracksDataException {
            try {
                instance.write(out);
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }

    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static ISerializerDeserializer<? extends Writable> createSerializerDeserializer(
            Class<? extends Writable> fClass) {
        return new WritableSerializerDeserializer(fClass);
    }

    public static RecordDescriptor createKeyValueRecordDescriptor(Class<? extends Writable> keyClass,
            Class<? extends Writable> valueClass) {
        @SuppressWarnings("rawtypes")
        ISerializerDeserializer[] fields = new ISerializerDeserializer[2];
        fields[0] = createSerializerDeserializer(keyClass);
        fields[1] = createSerializerDeserializer(valueClass);
        return new RecordDescriptor(fields);
    }

    public static RecordDescriptor createOneFieldRecordDescriptor(Class<? extends Writable> fieldClass) {
        @SuppressWarnings("rawtypes")
        ISerializerDeserializer[] fields = new ISerializerDeserializer[1];
        fields[0] = createSerializerDeserializer(fieldClass);
        return new RecordDescriptor(fields);
    }

    public static JobConf map2JobConf(Map<String, String> jobConfMap) {
        JobConf jobConf;
        synchronized (Configuration.class) {
            jobConf = new JobConf();
            for (Entry<String, String> entry : jobConfMap.entrySet()) {
                jobConf.set(entry.getKey(), entry.getValue());
            }
        }
        return jobConf;
    }

    public static Map<String, String> jobConf2Map(JobConf jobConf) {
        Map<String, String> jobConfMap = new HashMap<String, String>();
        for (Entry<String, String> entry : jobConf) {
            jobConfMap.put(entry.getKey(), entry.getValue());
        }
        return jobConfMap;
    }
}