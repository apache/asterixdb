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
package edu.uci.ics.pregelix.core.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.pregelix.core.runtime.touchpoint.WritableRecordDescriptorFactory;
import edu.uci.ics.pregelix.dataflow.base.IConfigurationFactory;
import edu.uci.ics.pregelix.dataflow.std.base.IAggregateFunctionFactory;
import edu.uci.ics.pregelix.dataflow.std.base.IRecordDescriptorFactory;
import edu.uci.ics.pregelix.dataflow.std.base.ISerializableAggregateFunctionFactory;
import edu.uci.ics.pregelix.dataflow.std.group.IClusteredAggregatorDescriptorFactory;
import edu.uci.ics.pregelix.runtime.agg.AccumulatingAggregatorFactory;
import edu.uci.ics.pregelix.runtime.agg.AggregationFunctionFactory;
import edu.uci.ics.pregelix.runtime.agg.SerializableAggregationFunctionFactory;
import edu.uci.ics.pregelix.runtime.agg.SerializableAggregatorDescriptorFactory;
import edu.uci.ics.pregelix.runtime.touchpoint.DatatypeHelper;

public class DataflowUtils {

    public enum AggregationMode {
        PARTIAL,
        FINAL
    }

    @SuppressWarnings("unchecked")
    public static RecordDescriptor getRecordDescriptorFromKeyValueClasses(Configuration conf, String className1,
            String className2) throws HyracksException {
        RecordDescriptor recordDescriptor = null;
        try {
            ClassLoader loader = DataflowUtils.class.getClassLoader();
            recordDescriptor = DatatypeHelper.createKeyValueRecordDescriptor(
                    (Class<? extends Writable>) loader.loadClass(className1),
                    (Class<? extends Writable>) loader.loadClass(className2), conf);
        } catch (ClassNotFoundException cnfe) {
            throw new HyracksException(cnfe);
        }
        return recordDescriptor;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static RecordDescriptor getRecordDescriptorFromWritableClasses(Configuration conf, String... classNames)
            throws HyracksException {
        RecordDescriptor recordDescriptor = null;
        ISerializerDeserializer[] serdes = new ISerializerDeserializer[classNames.length];
        ClassLoader loader = DataflowUtils.class.getClassLoader();
        try {
            int i = 0;
            for (String className : classNames)
                serdes[i++] = DatatypeHelper.createSerializerDeserializer(
                        (Class<? extends Writable>) loader.loadClass(className), conf, null);
        } catch (ClassNotFoundException cnfe) {
            throw new HyracksException(cnfe);
        }
        recordDescriptor = new RecordDescriptor(serdes);
        return recordDescriptor;
    }

    public static IRecordDescriptorFactory getWritableRecordDescriptorFactoryFromWritableClasses(
            IConfigurationFactory confFactory, String... classNames) throws HyracksException {
        IRecordDescriptorFactory rdFactory = new WritableRecordDescriptorFactory(confFactory, classNames);
        return rdFactory;
    }

    public static IClusteredAggregatorDescriptorFactory getAccumulatingAggregatorFactory(
            IConfigurationFactory confFactory, boolean isFinal, boolean partialAggAsInput) {
        IAggregateFunctionFactory aggFuncFactory = new AggregationFunctionFactory(confFactory, isFinal,
                partialAggAsInput);
        IClusteredAggregatorDescriptorFactory aggregatorFactory = new AccumulatingAggregatorFactory(
                new IAggregateFunctionFactory[] { aggFuncFactory });
        return aggregatorFactory;
    }

    public static IAggregatorDescriptorFactory getSerializableAggregatorFactory(IConfigurationFactory confFactory,
            boolean isFinal, boolean partialAggAsInput) {
        ISerializableAggregateFunctionFactory aggFuncFactory = new SerializableAggregationFunctionFactory(confFactory,
                partialAggAsInput);
        IAggregatorDescriptorFactory aggregatorFactory = new SerializableAggregatorDescriptorFactory(aggFuncFactory);
        return aggregatorFactory;
    }

    @SuppressWarnings("unchecked")
    public static RecordDescriptor getRecordDescriptorFromKeyValueClasses(IHyracksTaskContext ctx, Configuration conf,
            String className1, String className2) throws HyracksException {
        RecordDescriptor recordDescriptor = null;
        try {
            recordDescriptor = DatatypeHelper.createKeyValueRecordDescriptor((Class<? extends Writable>) ctx
                    .getJobletContext().loadClass(className1), (Class<? extends Writable>) ctx.getJobletContext()
                    .loadClass(className2), conf);
        } catch (Exception cnfe) {
            throw new HyracksException(cnfe);
        }
        return recordDescriptor;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static RecordDescriptor getRecordDescriptorFromWritableClasses(IHyracksTaskContext ctx, Configuration conf,
            String... classNames) throws HyracksException {
        RecordDescriptor recordDescriptor = null;
        ISerializerDeserializer[] serdes = new ISerializerDeserializer[classNames.length];
        try {
            int i = 0;
            for (String className : classNames) {
                Class<? extends Writable> c = (Class<? extends Writable>) ctx.getJobletContext().loadClass(className);
                serdes[i++] = DatatypeHelper.createSerializerDeserializer(c, conf, ctx);
                //System.out.println("thread " + Thread.currentThread().getId() + " after creating serde " + c.getClassLoader());
            }
        } catch (Exception cnfe) {
            throw new HyracksException(cnfe);
        }
        recordDescriptor = new RecordDescriptor(serdes);
        return recordDescriptor;
    }
}
