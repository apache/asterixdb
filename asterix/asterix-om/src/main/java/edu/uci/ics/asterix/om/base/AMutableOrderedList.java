package edu.uci.ics.asterix.om.base;

import java.util.ArrayList;

import edu.uci.ics.asterix.om.types.AOrderedListType;

public class AMutableOrderedList extends AOrderedList {

    public AMutableOrderedList(AOrderedListType type) {
        super(type);
    }

    public AMutableOrderedList(AOrderedListType type, ArrayList<IAObject> sequence) {
        super(type, sequence);
    }

    public void setValues(ArrayList<IAObject> values) {
        this.values = values;
    }

    public void add(IAObject obj) {
        values.add(obj);
    }

}
