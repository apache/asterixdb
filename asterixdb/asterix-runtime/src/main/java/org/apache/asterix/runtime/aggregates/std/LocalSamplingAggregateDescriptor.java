/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.runtime.aggregates.std;

import java.io.IOException;

import org.apache.asterix.common.config.CompilerProperties;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ABinary;
import org.apache.asterix.om.base.AMutableBinary;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.functions.IFunctionTypeInferer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.aggregates.base.AbstractAggregateFunctionDynamicDescriptor;
import org.apache.asterix.runtime.functions.FunctionTypeInferers;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

public class LocalSamplingAggregateDescriptor extends AbstractAggregateFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    private int numSamples;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new LocalSamplingAggregateDescriptor();
        }

        @Override
        public IFunctionTypeInferer createFunctionTypeInferer() {
            return FunctionTypeInferers.SET_NUM_SAMPLES;
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.LOCAL_SAMPLING;
    }

    @Override
    public void setImmutableStates(Object... states) {
        numSamples = (int) states[0];
    }

    @Override
    public IAggregateEvaluatorFactory createAggregateEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IAggregateEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IAggregateEvaluator createAggregateEvaluator(final IEvaluatorContext ctx)
                    throws HyracksDataException {
                return new LocalSamplingAggregateFunction(args, ctx, numSamples, sourceLoc);
            }
        };
    }

    private static class LocalSamplingAggregateFunction extends AbstractAggregateFunction {
        @SuppressWarnings("unchecked")
        private ISerializerDeserializer<ABinary> binarySerde =
                SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABINARY);
        private final AMutableBinary binary = new AMutableBinary(null, 0, 0);
        private final ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
        private final ArrayBackedValueStorage rangeMapBits = new ArrayBackedValueStorage();
        private final IPointable inputFieldValue = new VoidPointable();
        private final int numSamplesRequired;
        private final IScalarEvaluator[] sampledFieldsEval;
        private int numSamples;

        /**
         * @param args the fields that constitute a sample, e.g., $$1, $$2
         * @param context Hyracks task
         * @param numSamples number of samples to take
         * @param srcLoc source location
         * @throws HyracksDataException
         */
        private LocalSamplingAggregateFunction(IScalarEvaluatorFactory[] args, IEvaluatorContext context,
                int numSamples, SourceLocation srcLoc) throws HyracksDataException {
            super(srcLoc);
            sampledFieldsEval = new IScalarEvaluator[args.length];
            for (int i = 0; i < args.length; i++) {
                sampledFieldsEval[i] = args[i].createScalarEvaluator(context);
            }
            this.numSamplesRequired =
                    numSamples > 0 ? numSamples : (int) CompilerProperties.Option.COMPILER_SORT_SAMPLES.defaultValue();
        }

        @Override
        public void init() throws HyracksDataException {
            numSamples = 0;
            rangeMapBits.reset();
            // write a dummy integer at the beginning to be filled later with the actual number of samples taken
            IntegerSerializerDeserializer.write(0, rangeMapBits.getDataOutput());
        }

        /**
         * Receives data stream one tuple at a time from a data source and records samples.
         * @param tuple one sample
         * @throws HyracksDataException IO exception
         */
        @Override
        public void step(IFrameTupleReference tuple) throws HyracksDataException {
            if (numSamples >= numSamplesRequired) {
                return;
            }
            for (int i = 0; i < sampledFieldsEval.length; i++) {
                sampledFieldsEval[i].evaluate(tuple, inputFieldValue);
                IntegerSerializerDeserializer.write(inputFieldValue.getLength(), rangeMapBits.getDataOutput());
                rangeMapBits.append(inputFieldValue);
            }
            numSamples++;
        }

        /**
         * Sends the list of samples to the global range-map generator.
         * @param result will store the list of samples
         * @throws HyracksDataException IO exception
         */
        @Override
        public void finish(IPointable result) throws HyracksDataException {
            storage.reset();
            if (numSamples == 0) {
                // empty partition? then send system null as an indication of empty partition.
                try {
                    storage.getDataOutput().writeByte(ATypeTag.SERIALIZED_SYSTEM_NULL_TYPE_TAG);
                    result.set(storage);
                } catch (IOException e) {
                    throw HyracksDataException.create(e);
                }
            } else {
                IntegerPointable.setInteger(rangeMapBits.getByteArray(), rangeMapBits.getStartOffset(), numSamples);
                binary.setValue(rangeMapBits.getByteArray(), rangeMapBits.getStartOffset(), rangeMapBits.getLength());
                binarySerde.serialize(binary, storage.getDataOutput());
                result.set(storage);
            }
        }

        @Override
        public void finishPartial(IPointable result) throws HyracksDataException {
            finish(result);
        }
    }
}
