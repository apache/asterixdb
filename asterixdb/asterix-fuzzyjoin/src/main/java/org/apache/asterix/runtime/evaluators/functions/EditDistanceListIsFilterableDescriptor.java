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

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.dataflow.data.nontagged.serde.AOrderedListSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AUnorderedListSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * Checks whether a list with an edit distance threshold can be filtered with a lower bounding on the number
 * of common list elements. This function returns 'true' if the lower bound on the number of common elements
 * is positive, 'false' otherwise. For example, this function is used during an indexed nested-loop join based
 * on edit distance. We partition the tuples of the probing dataset into those that are filterable and those
 * that are not. Those that are filterable are forwarded to the index. The others are are fed into a (non
 * indexed) nested-loop join.
 */

@MissingNullInOutFunction
public class EditDistanceListIsFilterableDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;

    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new EditDistanceListIsFilterableDescriptor();
        }
    };

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(IEvaluatorContext ctx) throws HyracksDataException {
                return new EditDistanceListIsFilterableEvaluator(args, ctx);
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.EDIT_DISTANCE_LIST_IS_FILTERABLE;
    }

    private static class EditDistanceListIsFilterableEvaluator implements IScalarEvaluator {

        protected final IPointable listPtr = new VoidPointable();
        protected final IPointable edThreshPtr = new VoidPointable();
        protected final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
        protected final DataOutput output = resultStorage.getDataOutput();

        protected final IScalarEvaluator listEval;
        protected final IScalarEvaluator edThreshEval;

        @SuppressWarnings("unchecked")
        private final ISerializerDeserializer<ABoolean> booleanSerde =
                SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABOOLEAN);

        public EditDistanceListIsFilterableEvaluator(IScalarEvaluatorFactory[] args, IEvaluatorContext context)
                throws HyracksDataException {
            listEval = args[0].createScalarEvaluator(context);
            edThreshEval = args[1].createScalarEvaluator(context);
        }

        @Override
        public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
            resultStorage.reset();

            listEval.evaluate(tuple, listPtr);
            edThreshEval.evaluate(tuple, edThreshPtr);

            if (PointableHelper.checkAndSetMissingOrNull(result, listPtr, edThreshPtr)) {
                return;
            }

            // Check type and compute string length.
            byte[] bytes = listPtr.getByteArray();
            int offset = listPtr.getStartOffset();

            ATypeTag typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes[offset]);
            long listLen;
            switch (typeTag) {
                case MULTISET:
                    listLen = AUnorderedListSerializerDeserializer.getNumberOfItems(bytes, offset);
                    break;
                case ARRAY:
                    listLen = AOrderedListSerializerDeserializer.getNumberOfItems(bytes, offset);
                    break;
                default:
                    throw new TypeMismatchException(BuiltinFunctions.EDIT_DISTANCE_LIST_IS_FILTERABLE, 0,
                            ATypeTag.SERIALIZED_UNORDEREDLIST_TYPE_TAG, ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG);
            }

            // Check type and extract edit-distance threshold.
            bytes = edThreshPtr.getByteArray();
            offset = edThreshPtr.getStartOffset();
            long edThresh = ATypeHierarchy.getIntegerValue(BuiltinFunctions.EDIT_DISTANCE_LIST_IS_FILTERABLE.getName(),
                    1, bytes, offset);

            // Compute result.
            long lowerBound = listLen - edThresh;
            try {
                if (lowerBound <= 0) {
                    booleanSerde.serialize(ABoolean.FALSE, output);
                } else {
                    booleanSerde.serialize(ABoolean.TRUE, output);
                }
            } catch (IOException e) {
                throw HyracksDataException.create(e);
            }
            result.set(resultStorage);
        }
    }
}
