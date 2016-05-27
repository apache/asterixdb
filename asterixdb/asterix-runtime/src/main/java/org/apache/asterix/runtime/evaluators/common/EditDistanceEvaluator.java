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

import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.fuzzyjoin.similarity.SimilarityMetricEditDistance;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
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
    protected final SimilarityMetricEditDistance ed = new SimilarityMetricEditDistance();
    protected final AsterixOrderedListIterator firstOrdListIter = new AsterixOrderedListIterator();
    protected final AsterixOrderedListIterator secondOrdListIter = new AsterixOrderedListIterator();
    protected int editDistance = 0;
    protected final AMutableInt64 aInt64 = new AMutableInt64(-1);
    @SuppressWarnings("unchecked")
    protected final ISerializerDeserializer<AInt64> int64Serde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.AINT64);
    protected ATypeTag itemTypeTag;

    protected ATypeTag firstTypeTag;
    protected ATypeTag secondTypeTag;

    public EditDistanceEvaluator(IScalarEvaluatorFactory[] args, IHyracksTaskContext context)
            throws AlgebricksException {
        firstStringEval = args[0].createScalarEvaluator(context);
        secondStringEval = args[1].createScalarEvaluator(context);
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws AlgebricksException {
        resultStorage.reset();

        firstStringEval.evaluate(tuple, argPtr1);
        firstTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER
                .deserialize(argPtr1.getByteArray()[argPtr1.getStartOffset()]);
        secondStringEval.evaluate(tuple, argPtr2);
        secondTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER
                .deserialize(argPtr2.getByteArray()[argPtr2.getStartOffset()]);

        if (!checkArgTypes(firstTypeTag, secondTypeTag)) {
            result.set(resultStorage);
            return;
        }

        try {
            editDistance = computeResult(argPtr1, argPtr2, firstTypeTag);
        } catch (HyracksDataException e1) {
            throw new AlgebricksException(e1);
        }

        try {
            writeResult(editDistance);
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
        result.set(resultStorage);
    }

    protected int computeResult(IPointable left, IPointable right, ATypeTag argType)
            throws AlgebricksException, HyracksDataException {
        byte[] leftBytes = left.getByteArray();
        int leftStartOffset = left.getStartOffset();
        byte[] rightBytes = right.getByteArray();
        int rightStartOffset = right.getStartOffset();

        switch (argType) {
            case STRING: {
                return ed.UTF8StringEditDistance(leftBytes, leftStartOffset + typeIndicatorSize, rightBytes,
                        rightStartOffset + typeIndicatorSize);
            }
            case ORDEREDLIST: {
                firstOrdListIter.reset(leftBytes, leftStartOffset);
                secondOrdListIter.reset(rightBytes, rightStartOffset);
                try {
                    return (int) ed.getSimilarity(firstOrdListIter, secondOrdListIter);
                } catch (HyracksDataException e) {
                    throw new AlgebricksException(e);
                }
            }

            default: {
                throw new AlgebricksException(
                        "Invalid type " + argType + " passed as argument to edit distance function.");
            }

        }
    }

    protected boolean checkArgTypes(ATypeTag typeTag1, ATypeTag typeTag2) throws AlgebricksException {
        if (typeTag1 != typeTag2) {
            throw new AlgebricksException(
                    "Incompatible argument types given in edit distance: " + typeTag1 + " " + typeTag2);
        }

        // Since they are equal, check one tag is enough.
        if (typeTag1 != ATypeTag.STRING && typeTag1 != ATypeTag.ORDEREDLIST) { // could be an list
            throw new AlgebricksException(
                    "Only String or OrderedList type are allowed in edit distance, but given : " + typeTag1);
        }

        if (typeTag1 == ATypeTag.ORDEREDLIST) {
            itemTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER
                    .deserialize(argPtr1.getByteArray()[argPtr1.getStartOffset() + 1]);
            if (itemTypeTag == ATypeTag.ANY) {
                throw new AlgebricksException("\n Edit Distance can only be called on homogenous lists");
            }
            itemTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER
                    .deserialize(argPtr2.getByteArray()[argPtr2.getStartOffset() + 1]);
            if (itemTypeTag == ATypeTag.ANY) {
                throw new AlgebricksException("\n Edit Distance can only be called on homogenous lists");
            }
        }
        return true;
    }

    protected void writeResult(int ed) throws IOException {
        aInt64.setValue(ed);
        int64Serde.serialize(aInt64, out);
    }
}
