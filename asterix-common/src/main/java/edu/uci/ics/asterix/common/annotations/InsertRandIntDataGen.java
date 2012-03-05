package edu.uci.ics.asterix.common.annotations;

public class InsertRandIntDataGen implements IRecordFieldDataGen {

    private final String str1;
    private final String str2;

    public InsertRandIntDataGen(String str1, String str2) {
        this.str1 = str1;
        this.str2 = str2;
    }

    @Override
    public Kind getKind() {
        return Kind.INSERTRANDINT;
    }

    public String getStr1() {
        return str1;
    }

    public String getStr2() {
        return str2;
    }

}
