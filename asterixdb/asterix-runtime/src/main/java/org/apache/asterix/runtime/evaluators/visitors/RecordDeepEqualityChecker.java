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

import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.functions.BinaryHashMap;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.util.BinaryEntry;

class RecordDeepEqualityChecker {
    private final Pair<IVisitablePointable, Boolean> nestedVisitorArg =
            new Pair<IVisitablePointable, Boolean>(null, false);
    private final DeepEqualityVisitorHelper deepEqualityVisitorHelper = new DeepEqualityVisitorHelper();
    private DeepEqualityVisitor visitor;
    private BinaryEntry keyEntry = new BinaryEntry();
    private BinaryEntry valEntry = new BinaryEntry();
    private BinaryHashMap hashMap;

    public RecordDeepEqualityChecker(int tableSize, int tableFrameSize) {
        hashMap = deepEqualityVisitorHelper.initializeHashMap(tableSize, tableFrameSize, valEntry);
    }

    public RecordDeepEqualityChecker() {
        hashMap = deepEqualityVisitorHelper.initializeHashMap(valEntry);
    }

    public boolean accessRecord(IVisitablePointable recPointableLeft, IVisitablePointable recPointableRight,
            DeepEqualityVisitor visitor) throws HyracksDataException {

        if (recPointableLeft.equals(recPointableRight)) {
            return true;
        }

        this.visitor = visitor;

        hashMap.clear();

        ARecordVisitablePointable recLeft = (ARecordVisitablePointable) recPointableLeft;
        List<IVisitablePointable> fieldNamesLeft = recLeft.getFieldNames();

        ARecordVisitablePointable recRight = (ARecordVisitablePointable) recPointableRight;
        List<IVisitablePointable> fieldNamesRight = recRight.getFieldNames();

        int sizeLeft = fieldNamesLeft.size();
        int sizeRight = fieldNamesRight.size();
        if (sizeLeft != sizeRight) {
            return false;
        }

        // Build phase: Add items into hash map, starting with first record.
        for (int i = 0; i < sizeLeft; i++) {
            IVisitablePointable fieldName = fieldNamesLeft.get(i);
            keyEntry.set(fieldName.getByteArray(), fieldName.getStartOffset(), fieldName.getLength());
            IntegerPointable.setInteger(valEntry.getBuf(), 0, i);
            hashMap.put(keyEntry, valEntry);
        }

        return compareValues(recLeft.getFieldTypeTags(), recLeft.getFieldValues(), fieldNamesRight,
                recRight.getFieldTypeTags(), recRight.getFieldValues());
    }

    private boolean compareValues(List<IVisitablePointable> fieldTypesLeft, List<IVisitablePointable> fieldValuesLeft,
            List<IVisitablePointable> fieldNamesRight, List<IVisitablePointable> fieldTypesRight,
            List<IVisitablePointable> fieldValuesRight) throws HyracksDataException {

        // Probe phase: Probe items from second record
        for (int i = 0; i < fieldNamesRight.size(); i++) {
            IVisitablePointable fieldName = fieldNamesRight.get(i);
            keyEntry.set(fieldName.getByteArray(), fieldName.getStartOffset(), fieldName.getLength());
            BinaryEntry entry = hashMap.get(keyEntry);
            if (entry == null) {
                return false;
            }

            int fieldIdLeft = AInt32SerializerDeserializer.getInt(entry.getBuf(), entry.getOffset());
            ATypeTag fieldTypeLeft = PointableHelper.getTypeTag(fieldTypesLeft.get(fieldIdLeft));
            if (fieldTypeLeft.isDerivedType() && fieldTypeLeft != PointableHelper.getTypeTag(fieldTypesRight.get(i))) {
                return false;
            }
            nestedVisitorArg.first = fieldValuesRight.get(i);
            fieldValuesLeft.get(fieldIdLeft).accept(visitor, nestedVisitorArg);
            if (nestedVisitorArg.second == false) {
                return false;
            }
        }
        return true;
    }
}
