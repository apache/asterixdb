package edu.uci.ics.asterix.runtime.accessors.base;

import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.IAType;

public class DefaultOpenFieldType {

    // nested open field rec type
    public static ARecordType NESTED_OPEN_RECORD_TYPE = new ARecordType("nested-open", new String[] {}, new IAType[] {}, true);
}
