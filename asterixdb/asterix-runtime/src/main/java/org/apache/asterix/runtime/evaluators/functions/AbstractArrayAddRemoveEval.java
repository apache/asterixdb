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
import org.apache.asterix.builders.UnorderedListBuilder;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class AbstractArrayAddRemoveEval implements IScalarEvaluator {
    protected static final int RETURN_MISSING = -1;
    protected static final int RETURN_NULL = -2;

    private final IAType[] argTypes;
    private final ArrayBackedValueStorage storage;
    private final IPointable listArg;
    private final IPointable[] valuesArgs;
    private final IScalarEvaluator listArgEval;
    private final IScalarEvaluator[] valuesEval;
    private final SourceLocation sourceLocation;
    private final CastTypeEvaluator caster;
    private final ListAccessor listAccessor;
    private final int listOffset;
    private final int valuesOffset;
    private final boolean comparesValues;
    private final boolean makeOpen;
    private final boolean acceptNullValues;
    private IAsterixListBuilder orderedListBuilder;
    private IAsterixListBuilder unorderedListBuilder;

    public AbstractArrayAddRemoveEval(IScalarEvaluatorFactory[] args, IHyracksTaskContext ctx, int listOffset,
            int valuesOffset, int numValues, IAType[] argTypes, boolean comparesValues, SourceLocation sourceLocation,
            boolean makeOpen, boolean acceptNullValues) throws HyracksDataException {
        this.listOffset = listOffset;
        this.valuesOffset = valuesOffset;
        this.argTypes = argTypes;
        this.comparesValues = comparesValues;
        this.sourceLocation = sourceLocation;
        this.makeOpen = makeOpen;
        this.acceptNullValues = acceptNullValues;
        orderedListBuilder = null;
        unorderedListBuilder = null;
        listAccessor = new ListAccessor();
        caster = new CastTypeEvaluator();
        storage = new ArrayBackedValueStorage();
        listArg = new VoidPointable();
        listArgEval = args[listOffset].createScalarEvaluator(ctx);
        valuesArgs = new IPointable[numValues];
        valuesEval = new IScalarEvaluator[numValues];
        for (int i = 0; i < numValues; i++) {
            valuesArgs[i] = new VoidPointable();
            valuesEval[i] = args[i + valuesOffset].createScalarEvaluator(ctx);
        }
    }

    /**
     * @param listType the type of the list, ordered or unordered.
     * @param listArg the list into which to insert the items at the calculated returned position
     * @param tuple the tuple that contains the arguments including position argument
     * @return -1 if position value is missing, -2 if null, otherwise should return the adjusted position value, >= 0
     */
    protected abstract int getPosition(IFrameTupleReference tuple, IPointable listArg, ATypeTag listType)
            throws HyracksDataException;

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        // get the list argument, 1st or last argument, make sure it's a list
        listArgEval.evaluate(tuple, listArg);
        ATypeTag listArgTag = ATYPETAGDESERIALIZER.deserialize(listArg.getByteArray()[listArg.getStartOffset()]);

        // evaluate the position argument if provided by some functions
        int adjustedPosition = getPosition(tuple, listArg, listArgTag);

        if (listArgTag == ATypeTag.MISSING || adjustedPosition == RETURN_MISSING) {
            PointableHelper.setMissing(result);
            return;
        }

        boolean returnNull = false;
        if (!listArgTag.isListType() || adjustedPosition == RETURN_NULL) {
            returnNull = true;
        }

        // evaluate values to be added/removed
        ATypeTag valueTag;
        IAType defaultOpenType;
        boolean encounteredNonPrimitive = false;
        for (int i = 0; i < valuesEval.length; i++) {
            // cast val to open if needed. don't cast if function will return null anyway, e.g. list arg was not list
            defaultOpenType = DefaultOpenFieldType.getDefaultOpenFieldType(argTypes[i + valuesOffset].getTypeTag());
            if (defaultOpenType != null && !returnNull) {
                caster.reset(defaultOpenType, argTypes[i + valuesOffset], valuesEval[i]);
                caster.evaluate(tuple, valuesArgs[i]);
            } else {
                valuesEval[i].evaluate(tuple, valuesArgs[i]);
            }
            valueTag = ATYPETAGDESERIALIZER.deserialize(valuesArgs[i].getByteArray()[valuesArgs[i].getStartOffset()]);
            // for now, we don't support deep equality of object/lists. Throw an error if the value is of these types
            if (comparesValues && valueTag.isDerivedType()) {
                encounteredNonPrimitive = true;
            }
            if (valueTag == ATypeTag.MISSING) {
                PointableHelper.setMissing(result);
                return;
            }
            if (!acceptNullValues && valueTag == ATypeTag.NULL) {
                returnNull = true;
            }
        }

        if (returnNull) {
            PointableHelper.setNull(result);
            return;
        }

        if (encounteredNonPrimitive) {
            throw new RuntimeDataException(ErrorCode.CANNOT_COMPARE_COMPLEX, sourceLocation);
        }
        // all arguments are valid
        AbstractCollectionType listType = (AbstractCollectionType) argTypes[listOffset];
        IAsterixListBuilder listBuilder;
        // create the new list to be returned. cast the input list and make it open if required
        if (listArgTag == ATypeTag.ARRAY) {
            if (orderedListBuilder == null) {
                orderedListBuilder = new OrderedListBuilder();
            }
            listBuilder = orderedListBuilder;
            if (makeOpen) {
                listType = DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE;
                caster.reset(listType, argTypes[listOffset], listArgEval);
                caster.evaluate(tuple, listArg);
            }
        } else {
            if (unorderedListBuilder == null) {
                unorderedListBuilder = new UnorderedListBuilder();
            }
            listBuilder = unorderedListBuilder;
            if (makeOpen) {
                listType = DefaultOpenFieldType.NESTED_OPEN_AUNORDERED_LIST_TYPE;
                caster.reset(listType, argTypes[listOffset], listArgEval);
                caster.evaluate(tuple, listArg);
            }
        }

        listBuilder.reset(listType);
        listAccessor.reset(listArg.getByteArray(), listArg.getStartOffset());
        try {
            processList(listAccessor, listBuilder, valuesArgs, adjustedPosition);
            storage.reset();
            listBuilder.write(storage.getDataOutput(), true);
            result.set(storage);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    protected void processList(ListAccessor listAccessor, IAsterixListBuilder listBuilder, IPointable[] values,
            int position) throws IOException {
        int i;
        for (i = 0; i < position; i++) {
            storage.reset();
            listAccessor.writeItem(i, storage.getDataOutput());
            listBuilder.addItem(storage);
        }
        // insert the values arguments
        for (int j = 0; j < values.length; j++) {
            listBuilder.addItem(values[j]);
        }
        for (; i < listAccessor.size(); i++) {
            storage.reset();
            listAccessor.writeItem(i, storage.getDataOutput());
            listBuilder.addItem(storage);
        }
    }
}
