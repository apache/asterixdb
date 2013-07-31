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
import edu.uci.ics.pregelix.core.hadoop.config.ConfigurationFactory;
import edu.uci.ics.pregelix.core.runtime.touchpoint.WritableRecordDescriptorFactory;
import edu.uci.ics.pregelix.dataflow.group.IClusteredAggregatorDescriptorFactory;
import edu.uci.ics.pregelix.dataflow.std.base.IAggregateFunctionFactory;
import edu.uci.ics.pregelix.dataflow.std.base.IRecordDescriptorFactory;
import edu.uci.ics.pregelix.runtime.simpleagg.AccumulatingAggregatorFactory;
import edu.uci.ics.pregelix.runtime.simpleagg.AggregationFunctionFactory;
import edu.uci.ics.pregelix.runtime.touchpoint.DatatypeHelper;

public class DataflowUtils {

    public enum AggregationMode {
        PARTIAL,
        FINAL
    }

    @SuppressWarnings("unchecked")
    public static RecordDescriptor getRecordDescriptorFromKeyValueClasses(String className1, String className2)
            throws HyracksException {
        RecordDescriptor recordDescriptor = null;
        try {
            ClassLoader loader = DataflowUtils.class.getClassLoader();
            recordDescriptor = DatatypeHelper.createKeyValueRecordDescriptor(
                    (Class<? extends Writable>) loader.loadClass(className1),
                    (Class<? extends Writable>) loader.loadClass(className2));
        } catch (ClassNotFoundException cnfe) {
            throw new HyracksException(cnfe);
        }
        return recordDescriptor;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static RecordDescriptor getRecordDescriptorFromWritableClasses(String... classNames) throws HyracksException {
        RecordDescriptor recordDescriptor = null;
        ISerializerDeserializer[] serdes = new ISerializerDeserializer[classNames.length];
        ClassLoader loader = DataflowUtils.class.getClassLoader();
        try {
            int i = 0;
            for (String className : classNames)
                serdes[i++] = DatatypeHelper.createSerializerDeserializer((Class<? extends Writable>) loader
                        .loadClass(className));
        } catch (ClassNotFoundException cnfe) {
            throw new HyracksException(cnfe);
        }
        recordDescriptor = new RecordDescriptor(serdes);
        return recordDescriptor;
    }

    public static IRecordDescriptorFactory getWritableRecordDescriptorFactoryFromWritableClasses(String... classNames)
            throws HyracksException {
        IRecordDescriptorFactory rdFactory = new WritableRecordDescriptorFactory(classNames);
        return rdFactory;
    }

    public static IClusteredAggregatorDescriptorFactory getAccumulatingAggregatorFactory(Configuration conf,
            boolean isFinal, boolean partialAggAsInput) {
        IAggregateFunctionFactory aggFuncFactory = new AggregationFunctionFactory(new ConfigurationFactory(conf),
                isFinal, partialAggAsInput);
        IClusteredAggregatorDescriptorFactory aggregatorFactory = new AccumulatingAggregatorFactory(
                new IAggregateFunctionFactory[] { aggFuncFactory });
        return aggregatorFactory;
    }

    @SuppressWarnings("unchecked")
    public static RecordDescriptor getRecordDescriptorFromKeyValueClasses(IHyracksTaskContext ctx, String className1,
            String className2) throws HyracksException {
        RecordDescriptor recordDescriptor = null;
        try {
            recordDescriptor = DatatypeHelper.createKeyValueRecordDescriptor((Class<? extends Writable>) ctx
                    .getJobletContext().loadClass(className1), (Class<? extends Writable>) ctx.getJobletContext()
                    .loadClass(className2));
        } catch (Exception cnfe) {
            throw new HyracksException(cnfe);
        }
        return recordDescriptor;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static RecordDescriptor getRecordDescriptorFromWritableClasses(IHyracksTaskContext ctx, String... classNames)
            throws HyracksException {
        RecordDescriptor recordDescriptor = null;
        ISerializerDeserializer[] serdes = new ISerializerDeserializer[classNames.length];
        try {
            int i = 0;
            for (String className : classNames)
                serdes[i++] = DatatypeHelper.createSerializerDeserializer((Class<? extends Writable>) ctx
                        .getJobletContext().loadClass(className));
        } catch (Exception cnfe) {
            throw new HyracksException(cnfe);
        }
        recordDescriptor = new RecordDescriptor(serdes);
        return recordDescriptor;
    }
}
