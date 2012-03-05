package edu.uci.ics.asterix.optimizer.handle;

import edu.uci.ics.asterix.om.types.IAType;

public class FieldNameHandle implements IHandle {

    private String fieldName;
    private IAType fieldType;

    public FieldNameHandle(String fieldName) {
        this.fieldName = fieldName;
    }

    public IAType getFieldType() {
        return fieldType;
    }

    public void setFieldType(IAType fieldType) {
        this.fieldType = fieldType;
    }

    @Override
    public HandleType getHandleType() {
        return HandleType.FIELD_NAME;
    }

    public String getFieldName() {
        return fieldName;
    }
}
