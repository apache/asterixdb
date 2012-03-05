package edu.uci.ics.asterix.common.annotations;

import java.io.File;

public class ListValFileDataGen implements IRecordFieldDataGen {

    private final File file;
    private final int min;
    private final int max;

    public ListValFileDataGen(File file, int min, int max) {
        this.file = file;
        this.min = min;
        this.max = max;
    }

    @Override
    public Kind getKind() {
        return Kind.LISTVALFILE;
    }

    public File getFile() {
        return file;
    }

    public int getMin() {
        return min;
    }

    public int getMax() {
        return max;
    }

}
