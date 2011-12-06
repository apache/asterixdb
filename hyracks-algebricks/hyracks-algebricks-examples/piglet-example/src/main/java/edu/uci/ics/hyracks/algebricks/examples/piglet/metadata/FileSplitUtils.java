package edu.uci.ics.hyracks.algebricks.examples.piglet.metadata;

import java.io.File;

import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;

public class FileSplitUtils {
    public static FileSplit[] parseFileSplits(String fileSplits) {
        String[] splits = fileSplits.split(",");
        FileSplit[] fSplits = new FileSplit[splits.length];
        for (int i = 0; i < splits.length; ++i) {
            String s = splits[i].trim();
            int idx = s.indexOf(':');
            if (idx < 0) {
                throw new IllegalArgumentException("File split " + s + " not well formed");
            }
            fSplits[i] = new FileSplit(s.substring(0, idx), new FileReference(new File(s.substring(idx + 1))));
        }
        return fSplits;
    }
}