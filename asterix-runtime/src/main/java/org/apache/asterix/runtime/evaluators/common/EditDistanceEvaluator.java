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
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluator;
import org.apache.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IDataOutputProvider;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class EditDistanceEvaluator implements ICopyEvaluator {

    // assuming type indicator in serde format
    protected final int typeIndicatorSize = 1;

    protected final DataOutput out;
    protected final ArrayBackedValueStorage argOut = new ArrayBackedValueStorage();
    protected final ICopyEvaluator firstStringEval;
    protected final ICopyEvaluator secondStringEval;
    protected final SimilarityMetricEditDistance ed = new SimilarityMetricEditDistance();
    protected final AsterixOrderedListIterator firstOrdListIter = new AsterixOrderedListIterator();
    protected final AsterixOrderedListIterator secondOrdListIter = new AsterixOrderedListIterator();
    protected int editDistance = 0;
    protected final AMutableInt64 aInt64 = new AMutableInt64(-1);
    @SuppressWarnings("unchecked")
    protected final ISerializerDeserializer<AInt64> int64Serde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.AINT64);
    @SuppressWarnings("unchecked")
    private final ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ANULL);
    protected ATypeTag itemTypeTag;

    protected int firstStart = -1;
    protected int secondStart = -1;
    protected ATypeTag firstTypeTag;
    protected ATypeTag secondTypeTag;

    public EditDistanceEvaluator(ICopyEvaluatorFactory[] args, IDataOutputProvider output) throws AlgebricksException {
        out = output.getDataOutput();
        firstStringEval = args[0].createEvaluator(argOut);
        secondStringEval = args[1].createEvaluator(argOut);
    }

    @Override
    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {

        runArgEvals(tuple);

        if (!checkArgTypes(firstTypeTag, secondTypeTag))
            return;

        itemTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argOut.getByteArray()[firstStart + 1]);
        if (itemTypeTag == ATypeTag.ANY)
            throw new AlgebricksException("\n Edit Distance can only be called on homogenous lists");

        itemTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argOut.getByteArray()[secondStart + 1]);
        if (itemTypeTag == ATypeTag.ANY)
            throw new AlgebricksException("\n Edit Distance can only be called on homogenous lists");

        try {
            editDistance = computeResult(argOut.getByteArray(), firstStart, secondStart, firstTypeTag);
        } catch (HyracksDataException e1) {
            throw new AlgebricksException(e1);
        }

        try {
            writeResult(editDistance);
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
    }

    protected void runArgEvals(IFrameTupleReference tuple) throws AlgebricksException {
        argOut.reset();

        firstStart = argOut.getLength();
        firstStringEval.evaluate(tuple);
        secondStart = argOut.getLength();
        secondStringEval.evaluate(tuple);

        firstTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argOut.getByteArray()[firstStart]);
        secondTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argOut.getByteArray()[secondStart]);
    }

    protected int computeResult(byte[] bytes, int firstStart, int secondStart, ATypeTag argType)
            throws AlgebricksException, HyracksDataException {
        switch (argType) {

            case STRING: {
                return ed
                        .UTF8StringEditDistance(bytes, firstStart + typeIndicatorSize, secondStart + typeIndicatorSize);
            }

            case ORDEREDLIST: {
                firstOrdListIter.reset(bytes, firstStart);
                secondOrdListIter.reset(bytes, secondStart);
                try {
                    return (int) ed.getSimilarity(firstOrdListIter, secondOrdListIter);
                } catch (HyracksDataException e) {
                    throw new AlgebricksException(e);
                }
            }

            default: {
                throw new AlgebricksException("Invalid type " + argType
                        + " passed as argument to edit distance function.");
            }

        }
    }

    protected boolean checkArgTypes(ATypeTag typeTag1, ATypeTag typeTag2) throws AlgebricksException {
        // edit distance between null and anything else is undefined
        if (typeTag1 == ATypeTag.NULL || typeTag2 == ATypeTag.NULL) {
            try {
                nullSerde.serialize(ANull.NULL, out);
            } catch (IOException e) {
                throw new AlgebricksException(e);
            }
            return false;
        }

        if (typeTag1 != typeTag2) {
            throw new AlgebricksException("Incompatible argument types given in edit distance: " + typeTag1 + " "
                    + typeTag2);
        }

        return true;
    }

    protected void writeResult(int ed) throws IOException {
        aInt64.setValue(ed);
        int64Serde.serialize(aInt64, out);
    }
}
