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
import edu.uci.ics.asterix.runtime.accessors.AFlatValueAccessor;
import edu.uci.ics.asterix.runtime.accessors.AListAccessor;
import edu.uci.ics.asterix.runtime.accessors.ARecordAccessor;
import edu.uci.ics.asterix.runtime.accessors.base.DefaultOpenFieldType;
import edu.uci.ics.asterix.runtime.accessors.base.IBinaryAccessor;
import edu.uci.ics.asterix.runtime.util.container.IElementAllocator;
import edu.uci.ics.asterix.runtime.util.container.ListElementAllocator;

public class AccessorAllocator {

    private IElementAllocator<IBinaryAccessor, IAType> flatArtifactAllocator = new ListElementAllocator<IBinaryAccessor, IAType>(
            AFlatValueAccessor.FACTORY);
    private IElementAllocator<IBinaryAccessor, IAType> nestedRecValueAllocator = new ListElementAllocator<IBinaryAccessor, IAType>(
            ARecordAccessor.FACTORY);
    private IElementAllocator<IBinaryAccessor, IAType> nestedListValueAllocator = new ListElementAllocator<IBinaryAccessor, IAType>(
            AListAccessor.FACTORY);

    public IBinaryAccessor allocateFieldName() {
        return flatArtifactAllocator.allocate(null);
    }

    public IBinaryAccessor allocateFieldType() {
        return flatArtifactAllocator.allocate(null);
    }

    public IBinaryAccessor allocateFieldValue(IAType type) {
        if(type == null)
            return flatArtifactAllocator.allocate(null);
        else if (type.getTypeTag().equals(ATypeTag.RECORD))
            return nestedRecValueAllocator.allocate(type);
        else if (type.getTypeTag().equals(ATypeTag.UNORDEREDLIST) || type.getTypeTag().equals(ATypeTag.ORDEREDLIST))
            return nestedListValueAllocator.allocate(type);
        else
            return flatArtifactAllocator.allocate(null);
    }
    
    public IBinaryAccessor allocateFieldValue(ATypeTag typeTag) {
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

    public IBinaryAccessor allocateNestedListValue(IAType type) {
        return nestedListValueAllocator.allocate(type);
    }

    public void reset() {
        flatArtifactAllocator.reset();
        nestedRecValueAllocator.reset();
        nestedListValueAllocator.reset();
    }
}
