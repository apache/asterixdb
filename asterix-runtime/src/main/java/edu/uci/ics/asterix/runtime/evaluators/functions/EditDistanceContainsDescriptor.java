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

import java.io.DataOutput;
import edu.uci.ics.asterix.builders.OrderedListBuilder;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.fuzzyjoin.similarity.SimilarityMetricEditDistance;
import edu.uci.ics.asterix.om.base.ABoolean;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

public class EditDistanceContainsDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new EditDistanceContainsDescriptor();
        }
    };

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) throws AlgebricksException {
        return new ICopyEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(IDataOutputProvider output) throws AlgebricksException {
                return new EditDistanceContainsEvaluator(args, output);
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.EDIT_DISTANCE_CONTAINS;
    }

    private static class EditDistanceContainsEvaluator implements ICopyEvaluator {

        // assuming type indicator in serde format
        private final int typeIndicatorSize = 1;

        private DataOutput out;
        private final ArrayBackedValueStorage argOut = new ArrayBackedValueStorage();
        private final ICopyEvaluator evalString;
        private final ICopyEvaluator evalPattern;
        private final ICopyEvaluator evalEdThresh;
        private final SimilarityMetricEditDistance ed = new SimilarityMetricEditDistance();
        private int edThresh = -1;
        private final OrderedListBuilder listBuilder;
        private ArrayBackedValueStorage inputVal;
        protected final AMutableInt32 aInt32 = new AMutableInt32(-1);
        @SuppressWarnings("unchecked")
        private final ISerializerDeserializer<ABoolean> booleanSerde = AqlSerializerDeserializerProvider.INSTANCE
                .getSerializerDeserializer(BuiltinType.ABOOLEAN);
        @SuppressWarnings("unchecked")
        private final ISerializerDeserializer<AInt32> int32Serde = AqlSerializerDeserializerProvider.INSTANCE
                .getSerializerDeserializer(BuiltinType.AINT32);

        // allowed input types
        private final static byte SER_NULL_TYPE_TAG = ATypeTag.NULL.serialize();
        private final static byte SER_STRING_TYPE_TAG = ATypeTag.STRING.serialize();
        private final static byte SER_INT32_TYPE_TAG = ATypeTag.INT32.serialize();

        public EditDistanceContainsEvaluator(ICopyEvaluatorFactory[] args, IDataOutputProvider output)
                throws AlgebricksException {
            out = output.getDataOutput();
            evalString = args[0].createEvaluator(argOut);
            evalPattern = args[1].createEvaluator(argOut);
            evalEdThresh = args[2].createEvaluator(argOut);
            listBuilder = new OrderedListBuilder();
            inputVal = new ArrayBackedValueStorage();
        }

        @Override
        public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
            argOut.reset();
            int patternStart = argOut.getLength();
            evalString.evaluate(tuple);
            int stringStart = argOut.getLength();
            evalPattern.evaluate(tuple);
            int edThreshStart = argOut.getLength();
            evalEdThresh.evaluate(tuple);

            try {
                // edit distance between null and anything else is 0
                if (argOut.getByteArray()[stringStart] == SER_NULL_TYPE_TAG
                        || argOut.getByteArray()[patternStart] == SER_NULL_TYPE_TAG) {
                    writeResult(0);
                    return;
                }

                if (argOut.getByteArray()[stringStart] != SER_STRING_TYPE_TAG
                        || argOut.getByteArray()[patternStart] != SER_STRING_TYPE_TAG
                        || argOut.getByteArray()[edThreshStart] != SER_INT32_TYPE_TAG) {
                    throw new AlgebricksException(AsterixBuiltinFunctions.EDIT_DISTANCE_CONTAINS
                            + ": expects input type (STRING/NULL, STRING/NULL, INT), but got ("
                            + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argOut.getByteArray()[stringStart])
                            + ", "
                            + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argOut.getByteArray()[patternStart])
                            + ", "
                            + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argOut.getByteArray()[edThreshStart])
                            + ").");
                }
            } catch (HyracksDataException e) {
                throw new AlgebricksException(e);
            }

            edThresh = IntegerSerializerDeserializer.getInt(argOut.getByteArray(), edThreshStart + typeIndicatorSize);
            int minEd = ed.UTF8StringEditDistanceContains(argOut.getByteArray(), stringStart + typeIndicatorSize,
                    patternStart + typeIndicatorSize, edThresh);

            try {
                writeResult(minEd);
            } catch (HyracksDataException e) {
                throw new AlgebricksException(e);
            }
        }

        protected void writeResult(int minEd) throws HyracksDataException {
            boolean contains = (minEd < 0) ? false : true;
            listBuilder.reset(new AOrderedListType(BuiltinType.ANY, "list"));
            inputVal.reset();
            booleanSerde.serialize(contains ? ABoolean.TRUE : ABoolean.FALSE, inputVal.getDataOutput());
            listBuilder.addItem(inputVal);
            inputVal.reset();
            aInt32.setValue((contains) ? minEd : Integer.MAX_VALUE);
            int32Serde.serialize(aInt32, inputVal.getDataOutput());
            listBuilder.addItem(inputVal);
            listBuilder.write(out, true);
        }
    }
}
