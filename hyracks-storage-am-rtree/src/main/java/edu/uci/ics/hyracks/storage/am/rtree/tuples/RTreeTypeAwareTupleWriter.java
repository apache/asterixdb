package edu.uci.ics.hyracks.storage.am.rtree.tuples;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import edu.uci.ics.hyracks.storage.am.common.tuples.TypeAwareTupleWriter;

public class RTreeTypeAwareTupleWriter extends TypeAwareTupleWriter {

    public RTreeTypeAwareTupleWriter(ITypeTrait[] typeTraits) {
        super(typeTraits);
    }

    public int writeTupleFields(ITreeIndexTupleReference[] refs, int startField, ByteBuffer targetBuf, int targetOff) {
        int runner = targetOff;
        int nullFlagsBytes = getNullFlagsBytes(refs.length);
        // write null indicator bits
        for (int i = 0; i < nullFlagsBytes; i++) {
            targetBuf.put(runner++, (byte) 0);
        }

        // write field slots for variable length fields
        // since the r-tree has fixed length keys, we don't actually need this?
        encDec.reset(targetBuf.array(), runner);
        for (int i = startField; i < startField + refs.length; i++) {
            if (typeTraits[i].getStaticallyKnownDataLength() == ITypeTrait.VARIABLE_LENGTH) {
                encDec.encode(refs[i].getFieldLength(i));
            }
        }
        runner = encDec.getPos();

        // write data
        for (int i = 0; i < refs.length; i++) {
            System.arraycopy(refs[i].getFieldData(i), refs[i].getFieldStart(i), targetBuf.array(), runner,
                    refs[i].getFieldLength(i));
            runner += refs[i].getFieldLength(i);
        }
        return runner - targetOff;

    }
}
