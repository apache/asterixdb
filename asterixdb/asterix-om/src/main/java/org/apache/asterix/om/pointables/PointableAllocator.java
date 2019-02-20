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

package org.apache.asterix.om.pointables;

import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeTagUtil;
import org.apache.asterix.om.util.container.IObjectFactory;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * This class is the ONLY place to create IVisitablePointable object instances,
 * to enforce use of an object pool.
 */
public class PointableAllocator {

    private IObjectPool<AFlatValuePointable, IAType> flatValueAllocator =
            new ListObjectPool<>(AFlatValuePointable.FACTORY);
    private IObjectPool<ARecordVisitablePointable, IAType> recordValueAllocator =
            new ListObjectPool<>(ARecordVisitablePointable.FACTORY);
    private IObjectPool<AListVisitablePointable, IAType> listValueAllocator =
            new ListObjectPool<>(AListVisitablePointable.FACTORY);
    private IObjectPool<AOrderedListType, IAType> orederedListTypeAllocator =
            new ListObjectPool<>(new IObjectFactory<AOrderedListType, IAType>() {
                @Override
                public AOrderedListType create(IAType type) {
                    return new AOrderedListType(type, type.getTypeName() + "OrderedList");
                }
            });
    private IObjectPool<AOrderedListType, IAType> unorederedListTypeAllocator =
            new ListObjectPool<>(new IObjectFactory<AOrderedListType, IAType>() {
                @Override
                public AOrderedListType create(IAType type) {
                    return new AOrderedListType(type, type.getTypeName() + "UnorderedList");
                }
            });

    public AFlatValuePointable allocateEmpty() {
        return flatValueAllocator.allocate(null);
    }

    /**
     * This method should ONLY be used for long lasting IVisitablePointable.
     *
     * @return
     *         a generic type IVisitablePointable.
     */
    public static IVisitablePointable allocateUnrestableEmpty() {
        return AFlatValuePointable.FACTORY.create(null);
    }

    /**
     * allocate closed part value pointable
     *
     * @param type
     * @return the pointable object
     */
    public IVisitablePointable allocateFieldValue(IAType type) {
        if (type == null)
            return flatValueAllocator.allocate(null);
        else if (type.getTypeTag().equals(ATypeTag.OBJECT))
            return recordValueAllocator.allocate(type);
        else if (type.getTypeTag().equals(ATypeTag.MULTISET) || type.getTypeTag().equals(ATypeTag.ARRAY))
            return listValueAllocator.allocate(type);
        else
            return flatValueAllocator.allocate(null);
    }

    /**
     * allocate open part value pointable
     *
     * @param typeTag
     * @return the pointable object
     */
    public IVisitablePointable allocateFieldValue(ATypeTag typeTag, byte[] b, int offset) throws HyracksDataException {
        if (typeTag == null)
            return flatValueAllocator.allocate(null);
        else if (typeTag.equals(ATypeTag.OBJECT))
            return recordValueAllocator.allocate(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
        else if (typeTag.equals(ATypeTag.MULTISET)) {
            ATypeTag listItemType = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(b[offset]);
            if (listItemType == ATypeTag.ANY)
                return listValueAllocator.allocate(DefaultOpenFieldType.NESTED_OPEN_AUNORDERED_LIST_TYPE);
            else {
                if (listItemType.isDerivedType())
                    return allocateFieldValue(listItemType, b, offset + 1);
                else
                    return listValueAllocator.allocate(
                            unorederedListTypeAllocator.allocate(TypeTagUtil.getBuiltinTypeByTag(listItemType)));
            }
        } else if (typeTag.equals(ATypeTag.ARRAY)) {
            ATypeTag listItemType = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(b[offset]);
            if (listItemType == ATypeTag.ANY)
                return listValueAllocator.allocate(DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE);
            else {
                if (listItemType.isDerivedType())
                    return allocateFieldValue(listItemType, b, offset + 1);
                else
                    return listValueAllocator.allocate(
                            orederedListTypeAllocator.allocate(TypeTagUtil.getBuiltinTypeByTag(listItemType)));
            }
        } else
            return flatValueAllocator.allocate(null);
    }

    public AListVisitablePointable allocateListValue(IAType type) {
        return listValueAllocator.allocate(type);
    }

    public ARecordVisitablePointable allocateRecordValue(IAType type) {
        return recordValueAllocator.allocate(type);
    }

    public void freeRecord(ARecordVisitablePointable instance) {
        recordValueAllocator.free(instance);
    }

    public void reset() {
        flatValueAllocator.reset();
        recordValueAllocator.reset();
        listValueAllocator.reset();
    }
}
