package edu.uci.ics.asterix.om.base;

import java.util.ArrayList;

import edu.uci.ics.asterix.om.types.AUnorderedListType;

public final class AMutableUnorderedList extends AUnorderedList {

    public AMutableUnorderedList(AUnorderedListType type) {
        super(type);
    }

    public AMutableUnorderedList(AUnorderedListType type, ArrayList<IAObject> sequence) {
        super(type, sequence);
    }

    public void add(IAObject obj) {
        values.add(obj);
    }
}
