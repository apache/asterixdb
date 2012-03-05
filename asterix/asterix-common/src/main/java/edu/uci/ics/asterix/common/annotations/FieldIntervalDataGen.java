package edu.uci.ics.asterix.common.annotations;

public class FieldIntervalDataGen implements IRecordFieldDataGen {

    public enum ValueType {
        INT,
        LONG,
        FLOAT,
        DOUBLE
    }

    private final String min;
    private final String max;
    private final ValueType valueType;

    public FieldIntervalDataGen(ValueType valueType, String min, String max) {
        this.valueType = valueType;
        this.min = min;
        this.max = max;
    }

    @Override
    public Kind getKind() {
        return Kind.INTERVAL;
    }

    public String getMin() {
        return min;
    }

    public String getMax() {
        return max;
    }

    public ValueType getValueType() {
        return valueType;
    }

}
