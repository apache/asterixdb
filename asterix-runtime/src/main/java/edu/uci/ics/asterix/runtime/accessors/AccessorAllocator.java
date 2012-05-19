package edu.uci.ics.asterix.runtime.accessors;

import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.IAType;
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

    public IBinaryAccessor allocateNestedListValue(IAType type) {
        return nestedListValueAllocator.allocate(type);
    }

    public void reset() {
        flatArtifactAllocator.reset();
        nestedRecValueAllocator.reset();
        nestedListValueAllocator.reset();
    }
}
