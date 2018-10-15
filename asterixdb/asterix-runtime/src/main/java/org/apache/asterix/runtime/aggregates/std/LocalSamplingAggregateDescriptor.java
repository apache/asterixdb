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

import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.common.config.CompilerProperties;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.functions.IFunctionTypeInferer;
import org.apache.asterix.om.typecomputer.impl.ListOfSamplesTypeComputer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.runtime.aggregates.base.AbstractAggregateFunctionDynamicDescriptor;
import org.apache.asterix.runtime.functions.FunctionTypeInferers;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

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
            public IAggregateEvaluator createAggregateEvaluator(final IHyracksTaskContext ctx)
                    throws HyracksDataException {
                return new LocalSamplingAggregateFunction(args, ctx, numSamples);
            }
        };
    }

    private class LocalSamplingAggregateFunction implements IAggregateEvaluator {
        private final int numSamplesRequired;
        private final ArrayBackedValueStorage storage;
        private final IAsterixListBuilder listOfSamplesBuilder;
        private final IAsterixListBuilder oneSampleBuilder;
        private final IScalarEvaluator[] sampledFieldsEval;
        private final IPointable inputFieldValue;
        private int numSamplesTaken;

        /**
         * @param args the fields that constitute a sample, e.g., $$1, $$2
         * @param context Hyracks task
         * @throws HyracksDataException
         */
        private LocalSamplingAggregateFunction(IScalarEvaluatorFactory[] args, IHyracksTaskContext context,
                int numSamplesRequired) throws HyracksDataException {
            storage = new ArrayBackedValueStorage();
            inputFieldValue = new VoidPointable();
            sampledFieldsEval = new IScalarEvaluator[args.length];
            for (int i = 0; i < args.length; i++) {
                sampledFieldsEval[i] = args[i].createScalarEvaluator(context);
            }
            oneSampleBuilder = new OrderedListBuilder();
            listOfSamplesBuilder = new OrderedListBuilder();
            listOfSamplesBuilder.reset(ListOfSamplesTypeComputer.TYPE);
            this.numSamplesRequired = numSamplesRequired > 0 ? numSamplesRequired
                    : (int) CompilerProperties.Option.COMPILER_SORT_SAMPLES.defaultValue();
        }

        @Override
        public void init() throws HyracksDataException {
            numSamplesTaken = 0;
            listOfSamplesBuilder.reset(ListOfSamplesTypeComputer.TYPE);
        }

        /**
         * Receives data stream one tuple at a time from a data source and records samples.
         * @param tuple one sample
         * @throws HyracksDataException
         */
        @Override
        public void step(IFrameTupleReference tuple) throws HyracksDataException {
            if (numSamplesTaken >= numSamplesRequired) {
                return;
            }
            // start over for a new sample
            oneSampleBuilder.reset((AbstractCollectionType) ListOfSamplesTypeComputer.TYPE.getItemType());

            for (IScalarEvaluator fieldEval : sampledFieldsEval) {
                // add fields to make up one sample
                fieldEval.evaluate(tuple, inputFieldValue);
                oneSampleBuilder.addItem(inputFieldValue);
            }
            // prepare the sample to add it to the list of samples
            storage.reset();
            oneSampleBuilder.write(storage.getDataOutput(), true);
            listOfSamplesBuilder.addItem(storage);
            numSamplesTaken++;
        }

        /**
         * Sends the list of samples to the global range-map generator.
         * @param result list of samples
         * @throws HyracksDataException
         */
        @Override
        public void finish(IPointable result) throws HyracksDataException {
            storage.reset();
            if (numSamplesTaken == 0) {
                // empty partition? then send system null as an indication of empty partition.
                try {
                    storage.getDataOutput().writeByte(ATypeTag.SERIALIZED_SYSTEM_NULL_TYPE_TAG);
                    result.set(storage);
                } catch (IOException e) {
                    throw HyracksDataException.create(e);
                }
            } else {
                listOfSamplesBuilder.write(storage.getDataOutput(), true);
                result.set(storage);
            }
        }

        @Override
        public void finishPartial(IPointable result) throws HyracksDataException {
            finish(result);
        }
    }
}
