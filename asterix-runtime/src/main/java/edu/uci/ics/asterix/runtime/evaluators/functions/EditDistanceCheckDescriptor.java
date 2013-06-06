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
package edu.uci.ics.asterix.runtime.evaluators.functions;

import java.io.IOException;

import edu.uci.ics.asterix.builders.OrderedListBuilder;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ABoolean;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.common.EditDistanceEvaluator;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

public class EditDistanceCheckDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new EditDistanceCheckDescriptor();
        }
    };

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) throws AlgebricksException {
        return new ICopyEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(IDataOutputProvider output) throws AlgebricksException {
                return new EditDistanceCheckEvaluator(args, output);
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.EDIT_DISTANCE_CHECK;
    }

    private static class EditDistanceCheckEvaluator extends EditDistanceEvaluator {

        private final ICopyEvaluator edThreshEval;
        private int edThresh = -1;
        private final OrderedListBuilder listBuilder;
        private ArrayBackedValueStorage inputVal;
        @SuppressWarnings("unchecked")
        private final ISerializerDeserializer<ABoolean> booleanSerde = AqlSerializerDeserializerProvider.INSTANCE
                .getSerializerDeserializer(BuiltinType.ABOOLEAN);

        public EditDistanceCheckEvaluator(ICopyEvaluatorFactory[] args, IDataOutputProvider output)
                throws AlgebricksException {
            super(args, output);
            edThreshEval = args[2].createEvaluator(argOut);
            listBuilder = new OrderedListBuilder();
            inputVal = new ArrayBackedValueStorage();
        }

        @Override
        protected void runArgEvals(IFrameTupleReference tuple) throws AlgebricksException {
            super.runArgEvals(tuple);
            int edThreshStart = argOut.getLength();
            edThreshEval.evaluate(tuple);
            edThresh = IntegerSerializerDeserializer.getInt(argOut.getByteArray(), edThreshStart + typeIndicatorSize);
        }

        @Override
        protected int computeResult(byte[] bytes, int firstStart, int secondStart, ATypeTag argType)
                throws AlgebricksException {
            switch (argType) {

                case STRING: {
                    return ed.UTF8StringEditDistance(bytes, firstStart + typeIndicatorSize, secondStart
                            + typeIndicatorSize, edThresh);
                }

                case ORDEREDLIST: {
                    firstOrdListIter.reset(bytes, firstStart);
                    secondOrdListIter.reset(bytes, secondStart);
                    return (int) ed.getSimilarity(firstOrdListIter, secondOrdListIter, edThresh);
                }

                default: {
                    throw new AlgebricksException(AsterixBuiltinFunctions.EDIT_DISTANCE_CHECK.getName()
                            + ": expects input type as STRING or ORDEREDLIST but got " + argType + ".");
                }

            }
        }

        @Override
        protected void writeResult(int ed) throws IOException {

            listBuilder.reset(new AOrderedListType(BuiltinType.ANY, "list"));
            boolean matches = (ed < 0) ? false : true;
            inputVal.reset();
            booleanSerde.serialize(matches ? ABoolean.TRUE : ABoolean.FALSE, inputVal.getDataOutput());
            listBuilder.addItem(inputVal);

            inputVal.reset();
            aInt32.setValue((matches) ? ed : Integer.MAX_VALUE);
            int32Serde.serialize(aInt32, inputVal.getDataOutput());
            listBuilder.addItem(inputVal);

            listBuilder.write(out, true);
        }
    }

}
