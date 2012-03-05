package edu.uci.ics.asterix.common.annotations;

public class DatetimeAddRandHoursDataGen implements IRecordFieldDataGen {

    private final int minHour;
    private final int maxHour;
    private final String addToField;

    public DatetimeAddRandHoursDataGen(int minHour, int maxHour, String addToField) {
        this.minHour = minHour;
        this.maxHour = maxHour;
        this.addToField = addToField;
    }

    @Override
    public Kind getKind() {
        return Kind.DATETIMEADDRANDHOURS;
    }

    public int getMinHour() {
        return minHour;
    }

    public int getMaxHour() {
        return maxHour;
    }

    public String getAddToField() {
        return addToField;
    }

}
