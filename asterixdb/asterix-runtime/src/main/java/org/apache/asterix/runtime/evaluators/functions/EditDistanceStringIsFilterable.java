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
package org.apache.asterix.runtime.evaluators.functions;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.BooleanPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * Checks whether a string with an edit distance threshold can be filtered with a lower bounding on number of common grams.
 * This function returns 'true' if the lower bound on the number of common grams is positive, 'false' otherwise.
 * For example, this function is used during an indexed nested-loop join based on edit distance. We partition the tuples of the probing
 * dataset into those that are filterable and those that are not. Those that are filterable are forwarded to the index. The others are
 * are fed into a (non indexed) nested-loop join.
 */
public class EditDistanceStringIsFilterable extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new EditDistanceStringIsFilterable();
        }
    };

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args)
            throws AlgebricksException {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IHyracksTaskContext ctx) throws AlgebricksException {
                return new EditDistanceStringIsFilterableEvaluator(args, ctx);
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.EDIT_DISTANCE_STRING_IS_FILTERABLE;
    }

    private static class EditDistanceStringIsFilterableEvaluator implements IScalarEvaluator {

        protected final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
        protected final DataOutput output = resultStorage.getDataOutput();
        protected final IPointable argPtr = new VoidPointable();

        protected final IScalarEvaluator stringEval;
        protected final IScalarEvaluator edThreshEval;
        protected final IScalarEvaluator gramLenEval;
        protected final IScalarEvaluator usePrePostEval;

        @SuppressWarnings("unchecked")
        private final ISerializerDeserializer<ABoolean> booleanSerde = AqlSerializerDeserializerProvider.INSTANCE
                .getSerializerDeserializer(BuiltinType.ABOOLEAN);

        private final UTF8StringPointable utf8Ptr = new UTF8StringPointable();

        public EditDistanceStringIsFilterableEvaluator(IScalarEvaluatorFactory[] args, IHyracksTaskContext context)
                throws AlgebricksException {
            stringEval = args[0].createScalarEvaluator(context);
            edThreshEval = args[1].createScalarEvaluator(context);
            gramLenEval = args[2].createScalarEvaluator(context);
            usePrePostEval = args[3].createScalarEvaluator(context);
        }

        @Override
        public void evaluate(IFrameTupleReference tuple, IPointable result) throws AlgebricksException {
            resultStorage.reset();
            ATypeTag typeTag = null;

            // Check type and compute string length.
            stringEval.evaluate(tuple, argPtr);
            typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argPtr.getByteArray()[argPtr.getStartOffset()]);
            if (!typeTag.equals(ATypeTag.STRING)) {
                throw new AlgebricksException(AsterixBuiltinFunctions.EDIT_DISTANCE_STRING_IS_FILTERABLE.getName()
                        + ": expects input type STRING as first argument, but got " + typeTag + ".");
            }

            utf8Ptr.set(argPtr.getByteArray(), argPtr.getStartOffset() + 1, argPtr.getLength());
            int strLen = utf8Ptr.getStringLength();

            // Check type and extract edit-distance threshold.
            edThreshEval.evaluate(tuple, argPtr);
            typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argPtr.getByteArray()[argPtr.getStartOffset()]);

            long edThresh = 0;

            try {
                edThresh = ATypeHierarchy.getIntegerValue(argPtr.getByteArray(), argPtr.getStartOffset());
            } catch (HyracksDataException e1) {
                throw new AlgebricksException(e1);
            }

            // Check type and extract gram length.
            gramLenEval.evaluate(tuple, argPtr);
            typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argPtr.getByteArray()[argPtr.getStartOffset()]);

            long gramLen = 0;
            try {
                gramLen = ATypeHierarchy.getIntegerValue(argPtr.getByteArray(), argPtr.getStartOffset());
            } catch (HyracksDataException e1) {
                throw new AlgebricksException(e1);
            }

            // Check type and extract usePrePost flag.
            usePrePostEval.evaluate(tuple, argPtr);
            typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argPtr.getByteArray()[argPtr.getStartOffset()]);
            if (!typeTag.equals(ATypeTag.BOOLEAN)) {
                throw new AlgebricksException(AsterixBuiltinFunctions.EDIT_DISTANCE_STRING_IS_FILTERABLE.getName()
                        + ": expects input type BOOLEAN as fourth argument, but got " + typeTag + ".");
            }
            boolean usePrePost = BooleanPointable.getBoolean(argPtr.getByteArray(), argPtr.getStartOffset() + 1);

            // Compute result.
            long numGrams = (usePrePost) ? strLen + gramLen - 1 : strLen - gramLen + 1;
            long lowerBound = numGrams - edThresh * gramLen;
            try {
                if (lowerBound <= 0 || strLen == 0) {
                    booleanSerde.serialize(ABoolean.FALSE, output);
                } else {
                    booleanSerde.serialize(ABoolean.TRUE, output);
                }
            } catch (IOException e) {
                throw new AlgebricksException(e);
            }
            result.set(resultStorage);
        }
    }
}
