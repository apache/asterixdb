package edu.uci.ics.asterix.common.annotations;

import java.io.File;

public class FieldValFileDataGen implements IRecordFieldDataGen {

    private final File[] files;

    public FieldValFileDataGen(File[] files) {
        this.files = files;
    }

    @Override
    public Kind getKind() {
        return Kind.VALFILE;
    }

    public File[] getFiles() {
        return files;
    }

}
