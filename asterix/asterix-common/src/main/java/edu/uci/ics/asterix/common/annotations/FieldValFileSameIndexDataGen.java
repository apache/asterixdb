package edu.uci.ics.asterix.common.annotations;

import java.io.File;

public class FieldValFileSameIndexDataGen implements IRecordFieldDataGen {
    private final File file;
    private final String sameAsField;

    public FieldValFileSameIndexDataGen(File file, String sameAsField) {
        this.file = file;
        this.sameAsField = sameAsField;
    }

    @Override
    public Kind getKind() {
        return Kind.VALFILESAMEINDEX;
    }

    public File getFile() {
        return file;
    }

    public String getSameAsField() {
        return sameAsField;
    }

}
