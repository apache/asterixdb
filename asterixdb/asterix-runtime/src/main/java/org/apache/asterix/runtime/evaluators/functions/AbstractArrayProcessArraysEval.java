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

import org.apache.asterix.builders.AbvsBuilderFactory;
import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.UnorderedListBuilder;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class AbstractArrayProcessArraysEval implements IScalarEvaluator {
    private ArrayBackedValueStorage finalResult;
    private final ListAccessor listAccessor;
    private final IPointable[] listsArgs;
    private final IScalarEvaluator[] listsEval;
    private final SourceLocation sourceLocation;
    private final boolean isComparingElements;
    private final PointableAllocator pointableAllocator;
    private final IObjectPool<IMutableValueStorage, ATypeTag> storageAllocator;
    private final IAType[] argTypes;
    private final CastTypeEvaluator caster;
    private OrderedListBuilder orderedListBuilder;
    private UnorderedListBuilder unorderedListBuilder;

    public AbstractArrayProcessArraysEval(IScalarEvaluatorFactory[] args, IHyracksTaskContext ctx,
            boolean isComparingElements, SourceLocation sourceLoc, IAType[] argTypes) throws HyracksDataException {
        orderedListBuilder = null;
        unorderedListBuilder = null;
        pointableAllocator = new PointableAllocator();
        storageAllocator = new ListObjectPool<>(new AbvsBuilderFactory());
        finalResult = new ArrayBackedValueStorage();
        listAccessor = new ListAccessor();
        caster = new CastTypeEvaluator();
        listsArgs = new IPointable[args.length];
        listsEval = new IScalarEvaluator[args.length];
        for (int i = 0; i < args.length; i++) {
            listsArgs[i] = new VoidPointable();
            listsEval[i] = args[i].createScalarEvaluator(ctx);
        }
        sourceLocation = sourceLoc;
        this.isComparingElements = isComparingElements;
        this.argTypes = argTypes;
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        byte listArgType;
        boolean returnNull = false;
        AbstractCollectionType outList = null;
        ATypeTag listTag;
        IAType defaultOpenType;
        for (int i = 0; i < listsEval.length; i++) {
            listsEval[i].evaluate(tuple, listsArgs[i]);
            if (!returnNull) {
                listArgType = listsArgs[i].getByteArray()[listsArgs[i].getStartOffset()];
                listTag = ATYPETAGDESERIALIZER.deserialize(listArgType);
                if (!listTag.isListType()) {
                    returnNull = true;
                } else if (outList != null && outList.getTypeTag() != listTag) {
                    throw new RuntimeDataException(ErrorCode.DIFFERENT_LIST_TYPE_ARGS, sourceLocation);
                } else {
                    if (outList == null) {
                        outList = (AbstractCollectionType) DefaultOpenFieldType.getDefaultOpenFieldType(listTag);
                    }
                    defaultOpenType = DefaultOpenFieldType.getDefaultOpenFieldType(argTypes[i].getTypeTag());
                    caster.reset(defaultOpenType, argTypes[i], listsEval[i]);
                    caster.evaluate(tuple, listsArgs[i]);
                }
            }
        }

        if (returnNull) {
            PointableHelper.setNull(result);
            return;
        }

        IAsterixListBuilder listBuilder;
        if (outList.getTypeTag() == ATypeTag.ARRAY) {
            if (orderedListBuilder == null) {
                orderedListBuilder = new OrderedListBuilder();
            }
            listBuilder = orderedListBuilder;
        } else {
            if (unorderedListBuilder == null) {
                unorderedListBuilder = new UnorderedListBuilder();
            }
            listBuilder = unorderedListBuilder;
        }

        listBuilder.reset(outList);
        try {
            init();
            processLists(listsArgs, listBuilder);
            finish(listBuilder);

            finalResult.reset();
            listBuilder.write(finalResult.getDataOutput(), true);
            result.set(finalResult);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        } finally {
            release();
            storageAllocator.reset();
            pointableAllocator.reset();
        }
    }

    private void processLists(IPointable[] listsArgs, IAsterixListBuilder listBuilder) throws IOException {
        boolean itemInStorage;
        boolean isUsingItem;
        IPointable item = pointableAllocator.allocateEmpty();
        ArrayBackedValueStorage storage = (ArrayBackedValueStorage) storageAllocator.allocate(null);
        storage.reset();

        // process each list one by one
        for (int listIndex = 0; listIndex < listsArgs.length; listIndex++) {
            listAccessor.reset(listsArgs[listIndex].getByteArray(), listsArgs[listIndex].getStartOffset());
            // process the items of the current list
            for (int j = 0; j < listAccessor.size(); j++) {
                itemInStorage = listAccessor.getOrWriteItem(j, item, storage);
                if (ATYPETAGDESERIALIZER.deserialize(item.getByteArray()[item.getStartOffset()]).isDerivedType()
                        && isComparingElements) {
                    throw new RuntimeDataException(ErrorCode.CANNOT_COMPARE_COMPLEX, sourceLocation);
                }
                isUsingItem = processItem(item, listIndex, listBuilder);
                if (isUsingItem) {
                    item = pointableAllocator.allocateEmpty();
                    if (itemInStorage) {
                        storage = (ArrayBackedValueStorage) storageAllocator.allocate(null);
                        storage.reset();
                    }
                }
            }
        }
    }

    protected abstract void init();

    protected abstract void finish(IAsterixListBuilder listBuilder) throws HyracksDataException;

    protected abstract void release();

    protected abstract boolean processItem(IPointable item, int listIndex, IAsterixListBuilder listBuilder)
            throws HyracksDataException;
}
