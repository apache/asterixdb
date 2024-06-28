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

        AListVisitablePointable listRight = (AListVisitablePointable) listPointableRight;
        List<IVisitablePointable> itemsRight = listRight.getItems();

        if (itemsLeft.size() != itemsRight.size())
            return false;

        boolean isOrderedRight = listLeft.ordered();
        if (isOrderedRight != listRight.ordered())
            return false;

        if (isOrderedRight) {
            return processOrderedList(itemsLeft, itemsRight);
        } else {
            return processUnorderedList(itemsLeft, itemsRight);
        }
    }

    private boolean processOrderedList(List<IVisitablePointable> itemsLeft, List<IVisitablePointable> itemsRight)
            throws HyracksDataException {
        for (int i = 0; i < itemsLeft.size(); i++) {
            IVisitablePointable itemLeft = itemsLeft.get(i);
            IVisitablePointable itemRight = itemsRight.get(i);
            ATypeTag fieldTypeLeft = PointableHelper.getTypeTag(itemLeft);
            if (fieldTypeLeft.isDerivedType() && fieldTypeLeft != PointableHelper.getTypeTag(itemRight)) {
                return false;
            }
            itemVisitorArg.first = itemRight;
            itemLeft.accept(visitor, itemVisitorArg);
            if (itemVisitorArg.second == false)
                return false;
        }

        return true;
    }

    private boolean processUnorderedList(List<IVisitablePointable> itemsLeft, List<IVisitablePointable> itemsRight)
            throws HyracksDataException {

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

        return probeHashMap(itemsLeft, itemsRight);
    }

    private boolean probeHashMap(List<IVisitablePointable> itemsLeft, List<IVisitablePointable> itemsRight)
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
            IVisitablePointable itemLeft = itemsLeft.get(indexLeft);
            ATypeTag fieldTypeLeft = PointableHelper.getTypeTag(itemLeft);
            if (fieldTypeLeft.isDerivedType() && fieldTypeLeft != PointableHelper.getTypeTag(itemRight)) {
                return false;
            }

            itemVisitorArg.first = itemRight;
            itemLeft.accept(visitor, itemVisitorArg);
            if (itemVisitorArg.second == false)
                return false;
        }
        return true;
    }
}
