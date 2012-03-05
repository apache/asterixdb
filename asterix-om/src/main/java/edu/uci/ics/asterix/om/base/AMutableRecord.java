package edu.uci.ics.asterix.om.base;

import edu.uci.ics.asterix.om.types.ARecordType;

public class AMutableRecord extends ARecord {

    public AMutableRecord(ARecordType type, IAObject[] fields) {
        super(type, fields);
    }

    public void setValueAtPos(int pos, IAObject value) {
        fields[pos] = value;
    }

}
