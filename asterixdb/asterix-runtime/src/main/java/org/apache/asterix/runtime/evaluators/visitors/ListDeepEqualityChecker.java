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
package org.apache.asterix.runtime.evaluators.visitors;

import java.util.List;

import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.functions.BinaryHashMap;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.util.BinaryEntry;

class ListDeepEqualityChecker {
    private DeepEqualityVisitor visitor;

    private BinaryHashMap hashMap;
    private BinaryEntry keyEntry = new BinaryEntry();
    private BinaryEntry valEntry = new BinaryEntry();

    private final DeepEqualityVisitorHelper deepEqualityVisitorHelper = new DeepEqualityVisitorHelper();
    private final Pair<IVisitablePointable, Boolean> itemVisitorArg =
            new Pair<IVisitablePointable, Boolean>(null, false);

    public ListDeepEqualityChecker() {
        hashMap = deepEqualityVisitorHelper.initializeHashMap(valEntry);
    }

    public boolean accessList(IVisitablePointable listPointableLeft, IVisitablePointable listPointableRight,
            DeepEqualityVisitor visitor) throws HyracksDataException {
        this.visitor = visitor;

        AListVisitablePointable listLeft = (AListVisitablePointable) listPointableLeft;
        List<IVisitablePointable> itemsLeft = listLeft.getItems();
        List<IVisitablePointable> itemTagTypesLeft = listLeft.getItemTags();

        AListVisitablePointable listRight = (AListVisitablePointable) listPointableRight;
        List<IVisitablePointable> itemsRight = listRight.getItems();
        List<IVisitablePointable> itemTagTypesRight = listRight.getItemTags();

        if (itemsLeft.size() != itemsRight.size())
            return false;

        boolean isOrderedRight = listLeft.ordered();
        if (isOrderedRight != listRight.ordered())
            return false;

        if (isOrderedRight) {
            return processOrderedList(itemsLeft, itemTagTypesLeft, itemsRight, itemTagTypesRight);
        } else {
            return processUnorderedList(itemsLeft, itemTagTypesLeft, itemsRight, itemTagTypesRight);
        }
    }

    private boolean processOrderedList(List<IVisitablePointable> itemsLeft, List<IVisitablePointable> itemTagTypesLeft,
            List<IVisitablePointable> itemsRight, List<IVisitablePointable> itemTagTypesRight)
            throws HyracksDataException {
        for (int i = 0; i < itemsLeft.size(); i++) {
            ATypeTag fieldTypeLeft = PointableHelper.getTypeTag(itemTagTypesLeft.get(i));
            if (fieldTypeLeft.isDerivedType()
                    && fieldTypeLeft != PointableHelper.getTypeTag(itemTagTypesRight.get(i))) {
                return false;
            }
            itemVisitorArg.first = itemsRight.get(i);
            itemsLeft.get(i).accept(visitor, itemVisitorArg);
            if (itemVisitorArg.second == false)
                return false;
        }

        return true;
    }

    private boolean processUnorderedList(List<IVisitablePointable> itemsLeft,
            List<IVisitablePointable> itemTagTypesLeft, List<IVisitablePointable> itemsRight,
            List<IVisitablePointable> itemTagTypesRight) throws HyracksDataException {

        hashMap.clear();
        // Build phase: Add items into hash map, starting with first list.
        for (int i = 0; i < itemsLeft.size(); i++) {
            IVisitablePointable item = itemsLeft.get(i);
            byte[] buf = item.getByteArray();
            int off = item.getStartOffset();
            int len = item.getLength();
            keyEntry.set(buf, off, len);
            IntegerPointable.setInteger(valEntry.getBuf(), 0, i);
            hashMap.put(keyEntry, valEntry);
        }

        return probeHashMap(itemsLeft, itemTagTypesLeft, itemsRight, itemTagTypesRight);
    }

    private boolean probeHashMap(List<IVisitablePointable> itemsLeft, List<IVisitablePointable> itemTagTypesLeft,
            List<IVisitablePointable> itemsRight, List<IVisitablePointable> itemTagTypesRight)
            throws HyracksDataException {
        // Probe phase: Probe items from second list
        for (int indexRight = 0; indexRight < itemsRight.size(); indexRight++) {
            IVisitablePointable itemRight = itemsRight.get(indexRight);
            byte[] buf = itemRight.getByteArray();
            int off = itemRight.getStartOffset();
            int len = itemRight.getLength();
            keyEntry.set(buf, off, len);
            BinaryEntry entry = hashMap.get(keyEntry);

            // The items doesn't match
            if (entry == null) {
                return false;
            }

            int indexLeft = IntegerPointable.getInteger(entry.getBuf(), entry.getOffset());
            ATypeTag fieldTypeLeft = PointableHelper.getTypeTag(itemTagTypesLeft.get(indexLeft));
            if (fieldTypeLeft.isDerivedType()
                    && fieldTypeLeft != PointableHelper.getTypeTag(itemTagTypesRight.get(indexRight))) {
                return false;
            }

            itemVisitorArg.first = itemRight;
            itemsLeft.get(indexLeft).accept(visitor, itemVisitorArg);
            if (itemVisitorArg.second == false)
                return false;
        }
        return true;
    }
}
