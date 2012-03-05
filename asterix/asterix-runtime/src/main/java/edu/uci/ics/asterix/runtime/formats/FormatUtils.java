package edu.uci.ics.asterix.runtime.formats;

import edu.uci.ics.asterix.formats.base.IDataFormat;

public final class FormatUtils {
    public static IDataFormat getDefaultFormat() {
        return NonTaggedDataFormat.INSTANCE;
    }
}
