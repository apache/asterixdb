package edu.uci.ics.asterix.runtime.util;

import edu.uci.ics.hyracks.dataflow.common.comm.io.ByteArrayAccessibleOutputStream;

/**
 * This class extends ByteArrayAccessibleOutputStream to allow reset to a given
 * size.
 * 
 */
public class ResettableByteArrayOutputStream extends ByteArrayAccessibleOutputStream {

    public void reset(int size) {
        count = size;
    }
}
