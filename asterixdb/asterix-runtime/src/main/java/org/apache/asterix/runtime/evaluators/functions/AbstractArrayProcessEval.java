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

import java.io.IOException;
import java.util.List;

import org.apache.asterix.builders.AbvsBuilderFactory;
import org.apache.asterix.builders.ArrayListFactory;
import org.apache.asterix.builders.IAsterixListBuilder;
import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.builders.UnorderedListBuilder;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeTagUtil;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class AbstractArrayProcessEval implements IScalarEvaluator {
    private final AOrderedListType orderedListType;
    private final AUnorderedListType unorderedListType;
    private final ArrayBackedValueStorage storage;
    private final IScalarEvaluator listArgEval;
    private final ListAccessor listAccessor;
    private final IPointable listArg;
    private IAType inputListType;
    private IAsterixListBuilder orderedListBuilder;
    private IAsterixListBuilder unorderedListBuilder;

    protected final PointableAllocator pointableAllocator;
    protected final IObjectPool<IMutableValueStorage, ATypeTag> storageAllocator;
    protected final IObjectPool<List<IPointable>, ATypeTag> arrayListAllocator;

    public AbstractArrayProcessEval(IScalarEvaluatorFactory[] args, IEvaluatorContext ctx, IAType inputListType)
            throws HyracksDataException {
        orderedListBuilder = null;
        unorderedListBuilder = null;
        orderedListType = new AOrderedListType(BuiltinType.ANY, null);
        unorderedListType = new AUnorderedListType(BuiltinType.ANY, null);
        storage = new ArrayBackedValueStorage();
        listArg = new VoidPointable();
        pointableAllocator = new PointableAllocator();
        storageAllocator = new ListObjectPool<>(new AbvsBuilderFactory());
        arrayListAllocator = new ListObjectPool<>(new ArrayListFactory<>());
        listArgEval = args[0].createScalarEvaluator(ctx);
        listAccessor = new ListAccessor();
        this.inputListType = inputListType;
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        // get the list argument and make sure it's a list
        listArgEval.evaluate(tuple, listArg);

        if (PointableHelper.checkAndSetMissingOrNull(result, listArg)) {
            return;
        }

        byte listArgType = listArg.getByteArray()[listArg.getStartOffset()];

        // create the new list with the same type as the input list
        IAsterixListBuilder listBuilder;
        if (listArgType == ATypeTag.SERIALIZED_ORDEREDLIST_TYPE_TAG) {
            if (orderedListBuilder == null) {
                orderedListBuilder = new OrderedListBuilder();
            }
            listBuilder = orderedListBuilder;
        } else if (listArgType == ATypeTag.SERIALIZED_UNORDEREDLIST_TYPE_TAG) {
            if (unorderedListBuilder == null) {
                unorderedListBuilder = new UnorderedListBuilder();
            }
            listBuilder = unorderedListBuilder;
        } else {
            PointableHelper.setNull(result);
            return;
        }

        listAccessor.reset(listArg.getByteArray(), listArg.getStartOffset());
        AbstractCollectionType outputListType;
        if (!inputListType.getTypeTag().isListType()) {
            ATypeTag itemType = listAccessor.getItemType();
            if (listAccessor.getListType() == ATypeTag.ARRAY) {
                // TODO(ali): check the case when the item type from the runtime is a derived type
                orderedListType.setItemType(TypeTagUtil.getBuiltinTypeByTag(itemType));
                outputListType = orderedListType;
            } else {
                unorderedListType.setItemType(TypeTagUtil.getBuiltinTypeByTag(itemType));
                outputListType = unorderedListType;
            }
        } else {
            outputListType = (AbstractCollectionType) inputListType;
        }

        listBuilder.reset(outputListType);
        try {
            processList(listAccessor, listBuilder);
            storage.reset();
            listBuilder.write(storage.getDataOutput(), true);
            result.set(storage);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        } finally {
            pointableAllocator.reset();
            storageAllocator.reset();
            arrayListAllocator.reset();
        }
    }

    protected abstract void processList(ListAccessor listAccessor, IAsterixListBuilder listBuilder) throws IOException;
}
