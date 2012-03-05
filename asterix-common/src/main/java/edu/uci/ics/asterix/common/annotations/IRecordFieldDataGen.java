package edu.uci.ics.asterix.common.annotations;

public interface IRecordFieldDataGen {
    public enum Kind {
        AUTO,
        VALFILE,
        VALFILESAMEINDEX,
        INTERVAL,
        LIST,
        LISTVALFILE,
        INSERTRANDINT,
        DATEBETWEENYEARS,
        DATETIMEBETWEENYEARS,
        DATETIMEADDRANDHOURS
    }

    public Kind getKind();
}
