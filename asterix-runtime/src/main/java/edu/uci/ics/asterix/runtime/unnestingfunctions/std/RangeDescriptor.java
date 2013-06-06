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
package edu.uci.ics.asterix.runtime.unnestingfunctions.std;

import java.io.DataOutput;

import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.runtime.unnestingfunctions.base.AbstractUnnestingFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyUnnestingFunction;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyUnnestingFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

public class RangeDescriptor extends AbstractUnnestingFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new RangeDescriptor();
        }
    };

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.RANGE;
    }

    @Override
    public ICopyUnnestingFunctionFactory createUnnestingFunctionFactory(final ICopyEvaluatorFactory[] args)
            throws AlgebricksException {
        return new ICopyUnnestingFunctionFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public ICopyUnnestingFunction createUnnestingFunction(final IDataOutputProvider provider)
                    throws AlgebricksException {
                return new ICopyUnnestingFunction() {

                    private DataOutput out = provider.getDataOutput();
                    @SuppressWarnings("rawtypes")
                    private ISerializerDeserializer serde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.AINT32);
                    private ArrayBackedValueStorage inputVal = new ArrayBackedValueStorage();
                    private ICopyEvaluator eval0 = args[0].createEvaluator(inputVal);
                    private ICopyEvaluator eval1 = args[1].createEvaluator(inputVal);
                    private AMutableInt32 aInt32 = new AMutableInt32(0);
                    private int current;
                    private int max;

                    @Override
                    public void init(IFrameTupleReference tuple) throws AlgebricksException {
                        inputVal.reset();
                        eval0.evaluate(tuple);
                        current = IntegerSerializerDeserializer.getInt(inputVal.getByteArray(), 1);
                        inputVal.reset();
                        eval1.evaluate(tuple);
                        max = IntegerSerializerDeserializer.getInt(inputVal.getByteArray(), 1);
                    }

                    @SuppressWarnings("unchecked")
                    @Override
                    public boolean step() throws AlgebricksException {
                        if (current > max) {
                            return false;
                        }
                        aInt32.setValue(current);
                        try {
                            serde.serialize(aInt32, out);
                        } catch (HyracksDataException e) {
                            throw new AlgebricksException(e);
                        }
                        current++;
                        return true;
                    }

                };
            }
        };
    }

}
