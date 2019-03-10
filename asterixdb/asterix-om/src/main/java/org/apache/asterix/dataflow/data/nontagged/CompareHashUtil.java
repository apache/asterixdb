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
package org.apache.asterix.dataflow.data.nontagged;

import static org.apache.asterix.om.types.ATypeTag.SERIALIZED_MISSING_TYPE_TAG;
import static org.apache.asterix.om.types.ATypeTag.VALUE_TYPE_MAPPING;

import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeTagUtil;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class CompareHashUtil {

    private CompareHashUtil() {
    }

    public static Comparator<IVisitablePointable> createFieldNamesComp(IBinaryComparator stringComp) {
        return new Comparator<IVisitablePointable>() {
            @Override
            public int compare(IVisitablePointable name1, IVisitablePointable name2) {
                try {
                    return stringComp.compare(name1.getByteArray(), name1.getStartOffset() + 1, name1.getLength() - 1,
                            name2.getByteArray(), name2.getStartOffset() + 1, name2.getLength() - 1);
                } catch (HyracksDataException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    public static int addToHeap(List<IVisitablePointable> recordFNames, List<IVisitablePointable> recordFValues,
            PriorityQueue<IVisitablePointable> names) {
        // do not add fields whose value is missing, they don't exist in reality
        int length = recordFNames.size();
        IVisitablePointable fieldValue;
        int count = 0;
        for (int i = 0; i < length; i++) {
            fieldValue = recordFValues.get(i);
            if (fieldValue.getByteArray()[fieldValue.getStartOffset()] != SERIALIZED_MISSING_TYPE_TAG) {
                names.add(recordFNames.get(i));
                count++;
            }
        }
        return count;
    }

    public static int getIndex(List<IVisitablePointable> names, IVisitablePointable instance) {
        int size = names.size();
        for (int i = 0; i < size; i++) {
            if (instance == names.get(i)) {
                return i;
            }
        }
        throw new IllegalStateException();
    }

    public static IAType getType(ARecordType recordType, int fieldIdx, ATypeTag fieldTag) throws HyracksDataException {
        IAType[] fieldTypes = recordType.getFieldTypes();
        if (fieldIdx >= fieldTypes.length) {
            return fieldTag.isDerivedType() ? DefaultOpenFieldType.getDefaultOpenFieldType(fieldTag)
                    : TypeTagUtil.getBuiltinTypeByTag(fieldTag);
        }
        return fieldTypes[fieldIdx];
    }
}
