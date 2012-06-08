package edu.uci.ics.asterix.runtime.util;

import edu.uci.ics.hyracks.dataflow.common.comm.io.ByteArrayAccessibleOutputStream;

public class ResettableByteArrayOutputStream extends ByteArrayAccessibleOutputStream {

    public void reset(int size) {
        count = size;
    }
}
