package edu.uci.ics.asterix.common.annotations;

public class ListDataGen implements IRecordFieldDataGen {

    private final int min;
    private final int max;

    public ListDataGen(int min, int max) {
        this.min = min;
        this.max = max;
    }

    @Override
    public Kind getKind() {
        return Kind.LIST;
    }

    public int getMin() {
        return min;
    }

    public int getMax() {
        return max;
    }

}
