package edu.uci.ics.asterix.common.parse;

import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;

public interface IParseFileSplitsDecl {
    public boolean isDelimitedFileFormat();

    public Character getDelimChar();

    public FileSplit[] getSplits();
}
