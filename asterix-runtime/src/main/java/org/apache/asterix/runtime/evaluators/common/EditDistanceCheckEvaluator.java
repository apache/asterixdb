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

import java.io.IOException;

import edu.uci.ics.asterix.builders.OrderedListBuilder;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ABoolean;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.hierachy.ATypeHierarchy;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class EditDistanceCheckEvaluator extends EditDistanceEvaluator {

    protected final ICopyEvaluator edThreshEval;
    protected long edThresh = -1;
    protected final OrderedListBuilder listBuilder;
    protected ArrayBackedValueStorage listItemVal;
    @SuppressWarnings("unchecked")
    protected final ISerializerDeserializer<ABoolean> booleanSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ABOOLEAN);
    protected final static byte SER_INT32_TYPE_TAG = ATypeTag.INT32.serialize();

    public EditDistanceCheckEvaluator(ICopyEvaluatorFactory[] args, IDataOutputProvider output)
            throws AlgebricksException {
        super(args, output);
        edThreshEval = args[2].createEvaluator(argOut);
        listBuilder = new OrderedListBuilder();
        listItemVal = new ArrayBackedValueStorage();
    }

    @Override
    protected void runArgEvals(IFrameTupleReference tuple) throws AlgebricksException {
        super.runArgEvals(tuple);
        int edThreshStart = argOut.getLength();
        edThreshEval.evaluate(tuple);
        try {
            edThresh = ATypeHierarchy.getIntegerValue(argOut.getByteArray(), edThreshStart);
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }

    @Override
    protected int computeResult(byte[] bytes, int firstStart, int secondStart, ATypeTag argType)
            throws AlgebricksException, HyracksDataException {
        switch (argType) {

            case STRING: {
                return ed.UTF8StringEditDistance(bytes, firstStart + typeIndicatorSize,
                        secondStart + typeIndicatorSize, (int) edThresh);
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
        listItemVal.reset();
        booleanSerde.serialize(matches ? ABoolean.TRUE : ABoolean.FALSE, listItemVal.getDataOutput());
        listBuilder.addItem(listItemVal);

        listItemVal.reset();
        aInt64.setValue((matches) ? ed : Integer.MAX_VALUE);
        int64Serde.serialize(aInt64, listItemVal.getDataOutput());
        listBuilder.addItem(listItemVal);

        listBuilder.write(out, true);
    }
}
