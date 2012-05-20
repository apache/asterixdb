package edu.uci.ics.asterix.runtime.accessors.base;

import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.AUnorderedListType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;

public class DefaultOpenFieldType {

    // nested open field rec type
    public static ARecordType NESTED_OPEN_RECORD_TYPE = new ARecordType("nested-open", new String[] {},
            new IAType[] {}, true);

    // nested open list type
    public static AOrderedListType NESTED_OPEN_AORDERED_LIST_TYPE = new AOrderedListType(BuiltinType.ANY,
            "nested-ordered-list");

    // nested open list type
    public static AUnorderedListType NESTED_OPEN_AUNORDERED_LIST_TYPE = new AUnorderedListType(BuiltinType.ANY,
            "nested-unordered-list");

}
