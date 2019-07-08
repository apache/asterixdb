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
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.asterix.om.util.container.ObjectFactories;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class AbstractArrayProcessArraysEval implements IScalarEvaluator {
    private final ArrayBackedValueStorage finalResult;
    private final ListAccessor listAccessor;
    private final IPointable tempList;
    private final IPointable[] listsArgs;
    private final IScalarEvaluator[] listsEval;
    private final SourceLocation sourceLocation;
    private final IObjectPool<IPointable, Void> pointablePool;
    private final IObjectPool<IMutableValueStorage, Void> storageAllocator;
    private final IAType[] argTypes;
    private final CastTypeEvaluator caster;
    private OrderedListBuilder orderedListBuilder;
    private UnorderedListBuilder unorderedListBuilder;

    AbstractArrayProcessArraysEval(IScalarEvaluatorFactory[] args, IEvaluatorContext ctx, SourceLocation sourceLoc,
            IAType[] argTypes) throws HyracksDataException {
        orderedListBuilder = null;
        unorderedListBuilder = null;
        pointablePool = new ListObjectPool<>(ObjectFactories.VOID_FACTORY);
        storageAllocator = new ListObjectPool<>(ObjectFactories.STORAGE_FACTORY);
        finalResult = new ArrayBackedValueStorage();
        listAccessor = new ListAccessor();
        caster = new CastTypeEvaluator();
        tempList = new VoidPointable();
        listsArgs = new IPointable[args.length];
        listsEval = new IScalarEvaluator[args.length];
        for (int i = 0; i < args.length; i++) {
            listsArgs[i] = new VoidPointable();
            listsEval[i] = args[i].createScalarEvaluator(ctx);
        }
        sourceLocation = sourceLoc;
        this.argTypes = argTypes;
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        byte listArgType;
        boolean isReturnNull = false;
        AbstractCollectionType outList = null;
        ATypeTag listTag;

        try {
            for (int i = 0; i < listsEval.length; i++) {
                listsEval[i].evaluate(tuple, tempList);

                if (PointableHelper.checkAndSetMissingOrNull(result, tempList)) {
                    if (result.getByteArray()[0] == ATypeTag.SERIALIZED_MISSING_TYPE_TAG) {
                        return;
                    }

                    // null value, but check other arguments for missing first (higher priority)
                    isReturnNull = true;
                }

                if (!isReturnNull) {
                    listArgType = tempList.getByteArray()[tempList.getStartOffset()];
                    listTag = ATYPETAGDESERIALIZER.deserialize(listArgType);
                    if (!listTag.isListType()) {
                        isReturnNull = true;
                    } else if (outList != null && outList.getTypeTag() != listTag) {
                        throw new RuntimeDataException(ErrorCode.DIFFERENT_LIST_TYPE_ARGS, sourceLocation);
                    } else {
                        if (outList == null) {
                            outList = (AbstractCollectionType) DefaultOpenFieldType.getDefaultOpenFieldType(listTag);
                        }

                        caster.resetAndAllocate(outList, argTypes[i], listsEval[i]);
                        caster.cast(tempList, listsArgs[i]);
                    }
                }
            }

            if (isReturnNull) {
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
            pointablePool.reset();
            caster.deallocatePointables();
        }
    }

    private void processLists(IPointable[] listsArgs, IAsterixListBuilder listBuilder) throws IOException {
        boolean itemInStorage;
        boolean isUsingItem;
        IPointable item = pointablePool.allocate(null);
        ArrayBackedValueStorage storage = (ArrayBackedValueStorage) storageAllocator.allocate(null);
        storage.reset();

        // process each list one by one
        for (int listIndex = 0; listIndex < listsArgs.length; listIndex++) {
            listAccessor.reset(listsArgs[listIndex].getByteArray(), listsArgs[listIndex].getStartOffset());
            // process the items of the current list
            for (int j = 0; j < listAccessor.size(); j++) {
                itemInStorage = listAccessor.getOrWriteItem(j, item, storage);
                isUsingItem = processItem(item, listIndex, listBuilder);
                if (isUsingItem) {
                    item = pointablePool.allocate(null);
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
