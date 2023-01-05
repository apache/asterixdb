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

import static org.apache.asterix.om.types.EnumDeserializer.ATYPETAGDESERIALIZER;

import java.io.IOException;

import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeTagUtil;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/**
 * Abstract class for the ARRAY_MOVE and ARRAY_SWAP functions as they share a large portion of their code
 * with each other. Any classes that use this abstract one should override the `buildList` method, as it just
 * otherwise builds the same input list.
 */
public abstract class AbstractArrayMoveSwapEval implements IScalarEvaluator {

    private final ArrayBackedValueStorage storage;
    private final IScalarEvaluator listArgEval;
    private final IScalarEvaluator oldIndexEval;
    private final IScalarEvaluator newIndexEval;
    private final IPointable listArg;
    private final IPointable oldIndex;
    private final IPointable newIndex;
    private final ListAccessor listAccessor;
    private IAsterixListBuilder listBuilder;
    private String funcIdentifier;
    private IAType inputListType;

    AbstractArrayMoveSwapEval(IScalarEvaluatorFactory[] args, IEvaluatorContext ctx, String funcIdentifier,
            IAType inputListType) throws HyracksDataException {

        this.funcIdentifier = funcIdentifier;
        this.inputListType = inputListType;
        storage = new ArrayBackedValueStorage();
        listArgEval = args[0].createScalarEvaluator(ctx);
        oldIndexEval = args[1].createScalarEvaluator(ctx);
        newIndexEval = args[2].createScalarEvaluator(ctx);
        listArg = new VoidPointable();
        oldIndex = new VoidPointable();
        newIndex = new VoidPointable();
        listAccessor = new ListAccessor();
        listBuilder = new OrderedListBuilder();
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {

        storage.reset();

        // Check that our args aren't missing/null
        listArgEval.evaluate(tuple, listArg);
        oldIndexEval.evaluate(tuple, oldIndex);
        newIndexEval.evaluate(tuple, newIndex);
        if (PointableHelper.checkAndSetMissingOrNull(result, listArg, oldIndex, newIndex)) {
            return;
        }

        byte[] listBytes = listArg.getByteArray();
        int offset = listArg.getStartOffset();
        ATypeTag listType = ATYPETAGDESERIALIZER.deserialize(listBytes[offset]);

        byte[] oldIndexBytes = oldIndex.getByteArray();
        int oldIndexOffset = oldIndex.getStartOffset();
        ATypeTag oldIndexType = ATYPETAGDESERIALIZER.deserialize((oldIndexBytes[oldIndexOffset]));

        byte[] newIndexBytes = newIndex.getByteArray();
        int newIndexOffset = newIndex.getStartOffset();
        ATypeTag newIndexType = ATYPETAGDESERIALIZER.deserialize(newIndexBytes[newIndexOffset]);

        // Checks that the list is of ordered list type, and that the two indices are valid numeric values.
        // e.g) 1.0, 2, 4.0 works, but 4.5, 3.2 would not.
        if (!(listType == ATypeTag.ARRAY) || !ATypeHierarchy.isCompatible(oldIndexType, ATypeTag.DOUBLE)
                || !ATypeHierarchy.isCompatible(newIndexType, ATypeTag.DOUBLE)) {
            PointableHelper.setNull(result);
            return;
        }

        listAccessor.reset(listBytes, offset);

        AbstractCollectionType outputListType;

        ATypeTag listItemTypeTag = listAccessor.getItemType();
        IAType listItemType = TypeTagUtil.getBuiltinTypeByTag(listItemTypeTag);
        if (listAccessor.getListType() == ATypeTag.ARRAY) {
            outputListType = new AOrderedListType(listItemType, listItemType.getTypeName());
        }
        // Known list type, use it directly
        else {
            outputListType = (AbstractCollectionType) inputListType;
        }

        listBuilder.reset(outputListType);

        try {

            int listLen = listAccessor.size();

            double oldIndexVal = ATypeHierarchy.getDoubleValue(funcIdentifier, 1, oldIndexBytes, oldIndexOffset);
            double newIndexVal = ATypeHierarchy.getDoubleValue(funcIdentifier, 2, newIndexBytes, newIndexOffset);

            //Checks that old/new indices are within the range of the list and whether they are valid values
            if (Double.isNaN(oldIndexVal) || Double.isInfinite(oldIndexVal) || Math.floor(oldIndexVal) < oldIndexVal
                    || newIndexVal > (listLen - 1) || newIndexVal < -(listLen) || oldIndexVal < -(listLen)
                    || oldIndexVal > (listLen - 1)) {
                PointableHelper.setNull(result);
                return;
            }

            // Converting the indices values into integers to be used in iteration. Also accounting for the negative indices case by using modulo
            int oldIndexInt = (int) oldIndexVal;
            int newIndexInt = (int) newIndexVal;

            // use modulus to account for negative indices case
            oldIndexInt = (oldIndexInt + listLen) % listLen;
            newIndexInt = (newIndexInt + listLen) % listLen;

            // if no changes are to be made, then return original list
            if (oldIndexInt == newIndexInt || listLen <= 1) {
                result.set(listArg);
                return;
            }

            buildList(oldIndexInt, newIndexInt, listLen, listAccessor, listBuilder);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        storage.reset();
        listBuilder.write(storage.getDataOutput(), true);
        result.set(storage);
    }

    /**
     *
     * Default: Builds a list with the exact same values as the input list. Depending on the function that extends
     * this abstract class, this method should be overridden to suit the extending function.
     *
     * @param oldIndexInt - Position of the item at the old index.
     * @param newIndexInt - Position where the item at the old index wants to be.
     * @param listLen - to iterate through the list
     * @param listAccessor
     * @param listBuilder
     * @throws IOException
     */
    protected void buildList(int oldIndexInt, int newIndexInt, int listLen, ListAccessor listAccessor,
            IAsterixListBuilder listBuilder) throws IOException {
        for (int i = 0; i < listLen; i++) {
            storage.reset();
            listAccessor.writeItem(oldIndexInt, storage.getDataOutput());
            listBuilder.addItem(storage);
        }

    }

}
