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

import org.apache.asterix.dataflow.data.nontagged.serde.ABooleanSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class EditDistanceStringIsFilterableEvaluator implements IScalarEvaluator {

    protected final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    protected final DataOutput output = resultStorage.getDataOutput();
    protected final IPointable stringPtr = new VoidPointable();
    protected final IPointable edThreshPtr = new VoidPointable();
    protected final IPointable gramLenPtr = new VoidPointable();
    protected final IPointable usePrePostPtr = new VoidPointable();

    protected final IScalarEvaluator stringEval;
    protected final IScalarEvaluator edThreshEval;
    protected final IScalarEvaluator gramLenEval;
    protected final IScalarEvaluator usePrePostEval;

    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<ABoolean> booleanSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABOOLEAN);

    private final UTF8StringPointable utf8Ptr = new UTF8StringPointable();

    public EditDistanceStringIsFilterableEvaluator(IScalarEvaluatorFactory[] args, IEvaluatorContext context)
            throws HyracksDataException {
        stringEval = args[0].createScalarEvaluator(context);
        edThreshEval = args[1].createScalarEvaluator(context);
        gramLenEval = args[2].createScalarEvaluator(context);
        usePrePostEval = args[3].createScalarEvaluator(context);
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        resultStorage.reset();

        stringEval.evaluate(tuple, stringPtr);
        edThreshEval.evaluate(tuple, edThreshPtr);
        gramLenEval.evaluate(tuple, gramLenPtr);
        usePrePostEval.evaluate(tuple, usePrePostPtr);

        if (PointableHelper.checkAndSetMissingOrNull(result, stringPtr, edThreshPtr, gramLenPtr, usePrePostPtr)) {
            return;
        }

        // Check type and compute string length.
        byte typeTag = stringPtr.getByteArray()[stringPtr.getStartOffset()];
        if (typeTag != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
            throw new TypeMismatchException(BuiltinFunctions.EDIT_DISTANCE_STRING_IS_FILTERABLE, 0, typeTag,
                    ATypeTag.SERIALIZED_STRING_TYPE_TAG);
        }
        utf8Ptr.set(stringPtr.getByteArray(), stringPtr.getStartOffset() + 1, stringPtr.getLength());
        int strLen = utf8Ptr.getStringLength();

        // Check type and extract edit-distance threshold.
        long edThresh = ATypeHierarchy.getIntegerValue(BuiltinFunctions.EDIT_DISTANCE_LIST_IS_FILTERABLE.getName(), 1,
                edThreshPtr.getByteArray(), edThreshPtr.getStartOffset());

        // Check type and extract gram length.
        long gramLen = ATypeHierarchy.getIntegerValue(BuiltinFunctions.EDIT_DISTANCE_LIST_IS_FILTERABLE.getName(), 2,
                gramLenPtr.getByteArray(), gramLenPtr.getStartOffset());

        // Check type and extract usePrePost flag.
        typeTag = usePrePostPtr.getByteArray()[usePrePostPtr.getStartOffset()];
        if (typeTag != ATypeTag.SERIALIZED_BOOLEAN_TYPE_TAG) {
            throw new TypeMismatchException(BuiltinFunctions.EDIT_DISTANCE_STRING_IS_FILTERABLE, 3, typeTag,
                    ATypeTag.SERIALIZED_BOOLEAN_TYPE_TAG);
        }
        boolean usePrePost = ABooleanSerializerDeserializer.getBoolean(usePrePostPtr.getByteArray(),
                usePrePostPtr.getStartOffset() + 1);

        // Compute result.
        long numGrams = usePrePost ? strLen + gramLen - 1 : strLen - gramLen + 1;
        long lowerBound = numGrams - edThresh * gramLen;
        try {
            if (lowerBound <= 0 || strLen == 0) {
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
