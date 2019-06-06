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
package org.apache.asterix.om.types;

import org.apache.asterix.om.typecomputer.impl.TypeComputeUtils;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;

public class TypeHelper {

    private TypeHelper() {
    }

    public static boolean canBeMissing(IAType t) {
        switch (t.getTypeTag()) {
            case MISSING:
                return true;
            case UNION:
                return ((AUnionType) t).isMissableType();
            default:
                return false;
        }
    }

    public static boolean canBeUnknown(IAType t) {
        switch (t.getTypeTag()) {
            case MISSING:
            case NULL:
            case ANY:
                return true;
            case UNION:
                return ((AUnionType) t).isUnknownableType();
            default:
                return false;
        }
    }

    public static boolean isClosed(IAType t) {
        switch (t.getTypeTag()) {
            case MISSING:
            case ANY:
                return false;
            case UNION:
                return isClosed(((AUnionType) t).getActualType());
            default:
                return true;
        }
    }

    /**
     * Decides whether the {@param type} represents a type for data in the "opened up" form.
     * @param type type
     * @return true if the type will represent data in the "opened up" form. False, otherwise.
     */
    public static boolean isFullyOpen(IAType type) {
        IAType actualType = TypeComputeUtils.getActualType(type);
        switch (actualType.getTypeTag()) {
            case OBJECT:
                ARecordType recordType = (ARecordType) actualType;
                return recordType.getFieldNames().length == 0 && recordType.isOpen();
            case ARRAY:
            case MULTISET:
                AbstractCollectionType collectionType = (AbstractCollectionType) actualType;
                return TypeComputeUtils.getActualType(collectionType.getItemType()).getTypeTag() == ATypeTag.ANY;
            case ANY:
                return true;
            default:
                if (actualType.getTypeTag().isDerivedType()) {
                    throw new NotImplementedException();
                }
                // all other scalar types & ANY are open by nature and they don't need to be "opened up"
                return true;
        }
    }
}
