package edu.uci.ics.asterix.optimizer.handle;

import edu.uci.ics.asterix.om.types.IAType;

public class FieldIndexAndTypeHandle implements IHandle {

    private int fieldIndex;
    private IAType fieldType;

    public FieldIndexAndTypeHandle(int fieldIndex, IAType fieldType) {
        this.fieldIndex = fieldIndex;
        this.fieldType = fieldType;
    }

    @Override
    public HandleType getHandleType() {
        return HandleType.FIELD_INDEX_AND_TYPE;
    }

    public int getFieldIndex() {
        return fieldIndex;
    }

    public IAType getFieldType() {
        return fieldType;
    }
}
