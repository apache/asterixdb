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
package edu.uci.ics.asterix.runtime.evaluators.common;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.fuzzyjoin.similarity.SimilarityMetricEditDistance;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

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
    protected final AMutableInt32 aInt32 = new AMutableInt32(-1);
    @SuppressWarnings("unchecked")
    protected final ISerializerDeserializer<AInt32> int32Serde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.AINT32);
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

        editDistance = computeResult(argOut.getByteArray(), firstStart, secondStart, firstTypeTag);

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
            throws AlgebricksException {
        switch (argType) {

            case STRING: {
                return ed
                        .UTF8StringEditDistance(bytes, firstStart + typeIndicatorSize, secondStart + typeIndicatorSize);
            }

            case ORDEREDLIST: {
                firstOrdListIter.reset(bytes, firstStart);
                secondOrdListIter.reset(bytes, secondStart);
                return (int) ed.getSimilarity(firstOrdListIter, secondOrdListIter);
            }

            default: {
                throw new AlgebricksException("Invalid type " + argType
                        + " passed as argument to edit distance function.");
            }

        }
    }

    protected boolean checkArgTypes(ATypeTag typeTag1, ATypeTag typeTag2) throws AlgebricksException {
        // edit distance between null and anything else is 0
        if (typeTag1 == ATypeTag.NULL || typeTag2 == ATypeTag.NULL) {
            try {
                writeResult(0);
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
        aInt32.setValue(ed);
        int32Serde.serialize(aInt32, out);
    }
}
