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
package org.apache.asterix.runtime.evaluators.common;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.fuzzyjoin.similarity.SimilarityMetricEditDistance;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.asterix.runtime.exceptions.IncompatibleTypeException;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.asterix.runtime.exceptions.UnsupportedItemTypeException;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class EditDistanceEvaluator implements IScalarEvaluator {

    // assuming type indicator in serde format
    protected final int typeIndicatorSize = 1;

    protected final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    protected final DataOutput out = resultStorage.getDataOutput();
    protected final IPointable argPtr1 = new VoidPointable();
    protected final IPointable argPtr2 = new VoidPointable();
    protected final IScalarEvaluator firstStringEval;
    protected final IScalarEvaluator secondStringEval;
    protected final SourceLocation sourceLoc;
    protected final SimilarityMetricEditDistance ed = new SimilarityMetricEditDistance();
    protected final OrderedListIterator firstOrdListIter = new OrderedListIterator();
    protected final OrderedListIterator secondOrdListIter = new OrderedListIterator();
    protected int editDistance = 0;
    protected final AMutableInt64 aInt64 = new AMutableInt64(-1);
    @SuppressWarnings("unchecked")
    protected final ISerializerDeserializer<AInt64> int64Serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);
    protected ATypeTag itemTypeTag;

    protected ATypeTag firstTypeTag;
    protected ATypeTag secondTypeTag;

    public EditDistanceEvaluator(IScalarEvaluatorFactory[] args, IEvaluatorContext context, SourceLocation sourceLoc)
            throws HyracksDataException {
        firstStringEval = args[0].createScalarEvaluator(context);
        secondStringEval = args[1].createScalarEvaluator(context);
        this.sourceLoc = sourceLoc;
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        resultStorage.reset();
        firstStringEval.evaluate(tuple, argPtr1);
        secondStringEval.evaluate(tuple, argPtr2);

        if (PointableHelper.checkAndSetMissingOrNull(result, argPtr1, argPtr2)) {
            return;
        }

        firstTypeTag =
                EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argPtr1.getByteArray()[argPtr1.getStartOffset()]);
        secondTypeTag =
                EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argPtr2.getByteArray()[argPtr2.getStartOffset()]);

        if (!checkArgTypes(firstTypeTag, secondTypeTag)) {
            result.set(resultStorage);
            return;
        }

        editDistance = computeResult(argPtr1, argPtr2, firstTypeTag);
        try {
            writeResult(editDistance);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
        result.set(resultStorage);
    }

    protected int computeResult(IPointable left, IPointable right, ATypeTag argType) throws HyracksDataException {
        byte[] leftBytes = left.getByteArray();
        int leftStartOffset = left.getStartOffset();
        byte[] rightBytes = right.getByteArray();
        int rightStartOffset = right.getStartOffset();

        switch (argType) {
            case STRING: {
                // Passes -1 as the simThresh to calculate the edit distance
                // without applying any calculation optimizations.
                return ed.getActualUTF8StringEditDistanceVal(leftBytes, leftStartOffset + typeIndicatorSize, rightBytes,
                        rightStartOffset + typeIndicatorSize, -1);
            }
            case ARRAY: {
                firstOrdListIter.reset(leftBytes, leftStartOffset);
                secondOrdListIter.reset(rightBytes, rightStartOffset);
                return (int) ed.computeSimilarity(firstOrdListIter, secondOrdListIter);
            }
            default: {
                throw new TypeMismatchException(sourceLoc, BuiltinFunctions.EDIT_DISTANCE, 0, argType.serialize(),
                        ATypeTag.SERIALIZED_STRING_TYPE_TAG, ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG);
            }

        }
    }

    protected boolean checkArgTypes(ATypeTag typeTag1, ATypeTag typeTag2) throws HyracksDataException {
        if (typeTag1 != typeTag2) {
            throw new IncompatibleTypeException(sourceLoc, BuiltinFunctions.EDIT_DISTANCE, typeTag1.serialize(),
                    typeTag2.serialize());
        }

        // Since they are equal, check one tag is enough.
        if (typeTag1 != ATypeTag.STRING && typeTag1 != ATypeTag.ARRAY) { // could be an list
            throw new TypeMismatchException(sourceLoc, BuiltinFunctions.EDIT_DISTANCE, 0, typeTag1.serialize(),
                    ATypeTag.SERIALIZED_STRING_TYPE_TAG, ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG);
        }

        if (typeTag1 == ATypeTag.ARRAY) {
            itemTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER
                    .deserialize(argPtr1.getByteArray()[argPtr1.getStartOffset() + 1]);
            if (itemTypeTag == ATypeTag.ANY) {
                throw new UnsupportedItemTypeException(sourceLoc, BuiltinFunctions.EDIT_DISTANCE,
                        itemTypeTag.serialize());
            }
            itemTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER
                    .deserialize(argPtr2.getByteArray()[argPtr2.getStartOffset() + 1]);
            if (itemTypeTag == ATypeTag.ANY) {
                throw new UnsupportedItemTypeException(sourceLoc, BuiltinFunctions.EDIT_DISTANCE,
                        itemTypeTag.serialize());
            }
        }
        return true;
    }

    protected void writeResult(int ed) throws IOException {
        aInt64.setValue(ed);
        int64Serde.serialize(aInt64, out);
    }
}
