package edu.uci.ics.asterix.common.annotations;

public class DateBetweenYearsDataGen implements IRecordFieldDataGen {

    private final int minYear;
    private final int maxYear;

    public DateBetweenYearsDataGen(int minYear, int maxYear) {
        this.minYear = minYear;
        this.maxYear = maxYear;
    }

    @Override
    public Kind getKind() {
        return Kind.DATEBETWEENYEARS;
    }

    public int getMinYear() {
        return minYear;
    }

    public int getMaxYear() {
        return maxYear;
    }

}
