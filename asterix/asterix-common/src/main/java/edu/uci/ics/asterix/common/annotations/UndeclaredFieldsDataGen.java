package edu.uci.ics.asterix.common.annotations;

public class UndeclaredFieldsDataGen {

    public enum Type {
        INT
    }

    private final int minUndeclaredFields;
    private final int maxUndeclaredFields;
    private final String undeclaredFieldsPrefix;
    private final Type fieldType;

    public UndeclaredFieldsDataGen(Type fieldType, int minUndeclaredFields, int maxUndeclaredFields,
            String undeclaredFieldsPrefix) {
        this.fieldType = fieldType;
        this.minUndeclaredFields = minUndeclaredFields;
        this.maxUndeclaredFields = maxUndeclaredFields;
        this.undeclaredFieldsPrefix = undeclaredFieldsPrefix;
    }

    public int getMinUndeclaredFields() {
        return minUndeclaredFields;
    }

    public int getMaxUndeclaredFields() {
        return maxUndeclaredFields;
    }

    public String getUndeclaredFieldsPrefix() {
        return undeclaredFieldsPrefix;
    }

    public Type getFieldType() {
        return fieldType;
    }

}
