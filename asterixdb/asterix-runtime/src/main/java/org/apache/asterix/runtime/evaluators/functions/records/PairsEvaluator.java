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

package org.apache.asterix.runtime.evaluators.functions.records;

import java.io.DataOutput;
import java.util.ArrayDeque;
import java.util.List;

import org.apache.asterix.builders.OrderedListBuilder;
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

class PairsEvaluator extends AbstractRecordPairsEvaluator {

    // For writing output list
    private final OrderedListBuilder outerListBuilder = new OrderedListBuilder();

    // For writing each individual inner list.
    private final ArrayBackedValueStorage innerListStorage = new ArrayBackedValueStorage();
    private final DataOutput innerListOutput = innerListStorage.getDataOutput();
    private final OrderedListBuilder innerListBuilder = new OrderedListBuilder();

    private final PointableAllocator pAlloc = new PointableAllocator();
    private final ArrayDeque<IPointable> pNameQueue = new ArrayDeque<>();
    private final ArrayDeque<IPointable> pValueQueue = new ArrayDeque<>();

    private final VoidPointable nullPointable = new VoidPointable();

    PairsEvaluator(IScalarEvaluator eval0, IAType inputType) {
        super(eval0, inputType);
        PointableHelper.setNull(nullPointable);
    }

    @Override
    protected void buildOutput() throws HyracksDataException {
        pAlloc.reset();
        pNameQueue.clear();
        pValueQueue.clear();

        outerListBuilder.reset(AOrderedListType.FULL_OPEN_ORDEREDLIST_TYPE);

        addMembersToQueue(nullPointable, inputPointable);
        IPointable namePointable, valuePointable;
        while ((valuePointable = pValueQueue.poll()) != null) {
            namePointable = pNameQueue.remove();

            addMembersToQueue(namePointable, valuePointable);

            if (PointableHelper.getTypeTag(namePointable) != ATypeTag.NULL) {
                innerListStorage.reset();
                innerListBuilder.reset(AOrderedListType.FULL_OPEN_ORDEREDLIST_TYPE);
                innerListBuilder.addItem(namePointable);
                innerListBuilder.addItem(valuePointable);
                innerListBuilder.write(innerListOutput, true);

                outerListBuilder.addItem(innerListStorage);
            }
        }

        // Writes the result and sets the result pointable.
        outerListBuilder.write(resultOutput, true);
    }

    private void addMembersToQueue(IPointable namePointable, IPointable valuePointable) {
        ATypeTag valueTypeTag = PointableHelper.getTypeTag(valuePointable);
        switch (valueTypeTag) {
            case OBJECT:
                addRecordFieldsToQueue(valuePointable);
                break;
            case ARRAY:
            case MULTISET:
                addListItemsToQueue(valuePointable, DefaultOpenFieldType.getDefaultOpenFieldType(valueTypeTag),
                        namePointable);
                break;
        }
    }

    private void addRecordFieldsToQueue(IPointable recordPointable) {
        ARecordVisitablePointable visitablePointable =
                pAlloc.allocateRecordValue(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
        visitablePointable.set(recordPointable);
        List<IVisitablePointable> fieldNames = visitablePointable.getFieldNames();
        List<IVisitablePointable> fieldValues = visitablePointable.getFieldValues();
        for (int i = fieldNames.size() - 1; i >= 0; i--) {
            pNameQueue.push(fieldNames.get(i));
            pValueQueue.push(fieldValues.get(i));
        }
    }

    private void addListItemsToQueue(IPointable listPointable, IAType listType, IPointable fieldNamePointable) {
        AListVisitablePointable visitablePointable = pAlloc.allocateListValue(listType);
        visitablePointable.set(listPointable);
        List<IVisitablePointable> items = visitablePointable.getItems();
        for (int i = items.size() - 1; i >= 0; i--) {
            pNameQueue.push(fieldNamePointable);
            pValueQueue.push(items.get(i));
        }
    }

    @Override
    protected boolean validateInputType(ATypeTag inputTypeTag) {
        return inputTypeTag == ATypeTag.OBJECT || inputTypeTag == ATypeTag.ARRAY || inputTypeTag == ATypeTag.MULTISET;
    }
}
