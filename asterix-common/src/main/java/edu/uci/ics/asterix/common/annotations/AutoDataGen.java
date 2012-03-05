package edu.uci.ics.asterix.common.annotations;

public class AutoDataGen implements IRecordFieldDataGen {

    private final String initValueStr;

    public AutoDataGen(String initValueStr) {
        this.initValueStr = initValueStr;
    }

    @Override
    public Kind getKind() {
        return Kind.AUTO;
    }

    public String getInitValueStr() {
        return initValueStr;
    }

}
