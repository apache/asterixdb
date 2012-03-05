package edu.uci.ics.asterix.common.annotations;

public class DatetimeBetweenYearsDataGen implements IRecordFieldDataGen {

    private final int minYear;
    private final int maxYear;

    public DatetimeBetweenYearsDataGen(int minYear, int maxYear) {
        this.minYear = minYear;
        this.maxYear = maxYear;
    }

    @Override
    public Kind getKind() {
        return Kind.DATETIMEBETWEENYEARS;
    }

    public int getMinYear() {
        return minYear;
    }

    public int getMaxYear() {
        return maxYear;
    }

}
