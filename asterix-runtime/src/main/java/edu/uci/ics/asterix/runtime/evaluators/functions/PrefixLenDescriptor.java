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
import java.io.IOException;

import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.common.SimilarityFiltersCache;
import edu.uci.ics.fuzzyjoin.similarity.SimilarityFilters;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

public class PrefixLenDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    // allowed input types
    private final static byte SER_INT32_TYPE_TAG = ATypeTag.INT32.serialize();
    private final static byte SER_DOUBLE_TYPE_TAG = ATypeTag.DOUBLE.serialize();
    private final static byte SER_STRING_TYPE_TAG = ATypeTag.STRING.serialize();

    private final static FunctionIdentifier FID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "prefix-len@3",
            3);
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new PrefixLenDescriptor();
        }
    };

    @Override
    public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args) throws AlgebricksException {

        return new ICopyEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public ICopyEvaluator createEvaluator(final IDataOutputProvider output) throws AlgebricksException {

                return new ICopyEvaluator() {

                    private final DataOutput out = output.getDataOutput();
                    private final ArrayBackedValueStorage inputVal = new ArrayBackedValueStorage();
                    private final ICopyEvaluator evalLen = args[0].createEvaluator(inputVal);
                    private final ICopyEvaluator evalSimilarity = args[1].createEvaluator(inputVal);
                    private final ICopyEvaluator evalThreshold = args[2].createEvaluator(inputVal);

                    private final SimilarityFiltersCache similarityFiltersCache = new SimilarityFiltersCache();

                    // result
                    private final AMutableInt32 res = new AMutableInt32(0);
                    @SuppressWarnings("unchecked")
                    private final ISerializerDeserializer<AInt32> int32Serde = AqlSerializerDeserializerProvider.INSTANCE
                            .getSerializerDeserializer(BuiltinType.AINT32);

                    @Override
                    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        // length
                        inputVal.reset();
                        evalLen.evaluate(tuple);
                        if (inputVal.getByteArray()[0] != SER_INT32_TYPE_TAG) {
                            throw new AlgebricksException(FID.getName()
                                    + ": expects type Int32 for the first argument, but got "
                                    + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(inputVal.getByteArray()[0]));
                        }
                        int length = IntegerSerializerDeserializer.getInt(inputVal.getByteArray(), 1);

                        // similarity threshold
                        inputVal.reset();
                        evalThreshold.evaluate(tuple);
                        if (inputVal.getByteArray()[0] != SER_DOUBLE_TYPE_TAG) {
                            throw new AlgebricksException(FID.getName()
                                    + ": expects type DOUBLE for the second argument, but got "
                                    + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(inputVal.getByteArray()[0]));
                        }
                        float similarityThreshold = (float) ADoubleSerializerDeserializer.getDouble(
                                inputVal.getByteArray(), 1);

                        // similarity name
                        inputVal.reset();
                        evalSimilarity.evaluate(tuple);
                        if (inputVal.getByteArray()[0] != SER_STRING_TYPE_TAG) {
                            throw new AlgebricksException(FID.getName()
                                    + ": expects type STRING for the third argument, but got "
                                    + EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(inputVal.getByteArray()[0]));
                        }
                        SimilarityFilters similarityFilters = similarityFiltersCache.get(similarityThreshold,
                                inputVal.getByteArray());

                        int prefixLength = similarityFilters.getPrefixLength(length);
                        res.setValue(prefixLength);

                        try {
                            int32Serde.serialize(res, out);
                        } catch (IOException e) {
                            throw new AlgebricksException(e);
                        }
                    }
                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return FID;
    }

}
