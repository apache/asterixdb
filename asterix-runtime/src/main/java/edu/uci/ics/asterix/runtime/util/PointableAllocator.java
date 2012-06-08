/*
 * Copyright 2009-2010 by The Regents of the University of California
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

package edu.uci.ics.asterix.runtime.util;

import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.runtime.pointables.AFlatValuePointable;
import edu.uci.ics.asterix.runtime.pointables.AListPointable;
import edu.uci.ics.asterix.runtime.pointables.ARecordPointable;
import edu.uci.ics.asterix.runtime.pointables.base.DefaultOpenFieldType;
import edu.uci.ics.asterix.runtime.pointables.base.IVisitablePointable;
import edu.uci.ics.asterix.runtime.util.container.IElementAllocator;
import edu.uci.ics.asterix.runtime.util.container.ListElementAllocator;

public class PointableAllocator {

    private IElementAllocator<IVisitablePointable, IAType> flatArtifactAllocator = new ListElementAllocator<IVisitablePointable, IAType>(
            AFlatValuePointable.FACTORY);
    private IElementAllocator<IVisitablePointable, IAType> nestedRecValueAllocator = new ListElementAllocator<IVisitablePointable, IAType>(
            ARecordPointable.FACTORY);
    private IElementAllocator<IVisitablePointable, IAType> nestedListValueAllocator = new ListElementAllocator<IVisitablePointable, IAType>(
            AListPointable.FACTORY);

    public IVisitablePointable allocateFieldName() {
        return flatArtifactAllocator.allocate(null);
    }

    public IVisitablePointable allocateFieldType() {
        return flatArtifactAllocator.allocate(null);
    }

    public IVisitablePointable allocateFieldValue(IAType type) {
        if(type == null)
            return flatArtifactAllocator.allocate(null);
        else if (type.getTypeTag().equals(ATypeTag.RECORD))
            return nestedRecValueAllocator.allocate(type);
        else if (type.getTypeTag().equals(ATypeTag.UNORDEREDLIST) || type.getTypeTag().equals(ATypeTag.ORDEREDLIST))
            return nestedListValueAllocator.allocate(type);
        else
            return flatArtifactAllocator.allocate(null);
    }
    
    public IVisitablePointable allocateFieldValue(ATypeTag typeTag) {
        if(typeTag == null)
            return flatArtifactAllocator.allocate(null);
        else if (typeTag.equals(ATypeTag.RECORD))
            return nestedRecValueAllocator.allocate(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
        else if (typeTag.equals(ATypeTag.UNORDEREDLIST))
            return nestedListValueAllocator.allocate(DefaultOpenFieldType.NESTED_OPEN_AUNORDERED_LIST_TYPE);
        else if(typeTag.equals(ATypeTag.ORDEREDLIST))
            return nestedListValueAllocator.allocate(DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE);
        else
            return flatArtifactAllocator.allocate(null);
    }

    public IVisitablePointable allocateNestedListValue(IAType type) {
        return nestedListValueAllocator.allocate(type);
    }

    public void reset() {
        flatArtifactAllocator.reset();
        nestedRecValueAllocator.reset();
        nestedListValueAllocator.reset();
    }
}
