/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.asterix.om.pointables;

import edu.uci.ics.asterix.om.pointables.base.DefaultOpenFieldType;
import edu.uci.ics.asterix.om.pointables.base.IVisitablePointable;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.container.IObjectPool;
import edu.uci.ics.asterix.om.util.container.ListObjectPool;

/**
 * This class is the ONLY place to create IVisitablePointable object instances,
 * to enforce use of an object pool.
 */
public class PointableAllocator {

    private IObjectPool<IVisitablePointable, IAType> flatValueAllocator = new ListObjectPool<IVisitablePointable, IAType>(
            AFlatValuePointable.FACTORY);
    private IObjectPool<IVisitablePointable, IAType> recordValueAllocator = new ListObjectPool<IVisitablePointable, IAType>(
            ARecordPointable.FACTORY);
    private IObjectPool<IVisitablePointable, IAType> listValueAllocator = new ListObjectPool<IVisitablePointable, IAType>(
            AListPointable.FACTORY);

    public IVisitablePointable allocateEmpty() {
        return flatValueAllocator.allocate(null);
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
        else if (type.getTypeTag().equals(ATypeTag.RECORD))
            return recordValueAllocator.allocate(type);
        else if (type.getTypeTag().equals(ATypeTag.UNORDEREDLIST) || type.getTypeTag().equals(ATypeTag.ORDEREDLIST))
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
    public IVisitablePointable allocateFieldValue(ATypeTag typeTag) {
        if (typeTag == null)
            return flatValueAllocator.allocate(null);
        else if (typeTag.equals(ATypeTag.RECORD))
            return recordValueAllocator.allocate(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
        else if (typeTag.equals(ATypeTag.UNORDEREDLIST))
            return listValueAllocator.allocate(DefaultOpenFieldType.NESTED_OPEN_AUNORDERED_LIST_TYPE);
        else if (typeTag.equals(ATypeTag.ORDEREDLIST))
            return listValueAllocator.allocate(DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE);
        else
            return flatValueAllocator.allocate(null);
    }

    public IVisitablePointable allocateListValue(IAType type) {
        return listValueAllocator.allocate(type);
    }

    public IVisitablePointable allocateRecordValue(IAType type) {
        return recordValueAllocator.allocate(type);
    }

    public void reset() {
        flatValueAllocator.reset();
        recordValueAllocator.reset();
        listValueAllocator.reset();
    }
}
