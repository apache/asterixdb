package edu.uci.ics.asterix.common.annotations;

public interface IRecordTypeAnnotation {
    public enum Kind {
        RECORD_DATA_GEN
    }

    public Kind getKind();
}
