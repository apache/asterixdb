package edu.uci.ics.asterix.om.base;

import java.util.ArrayList;

public class ACollectionCursor implements IACursor {

    private ArrayList<IAObject> values;
    private int pos;
    private int size;
    private IAObject object = null;

    public ACollectionCursor() {
    }

    public ACollectionCursor(AUnorderedList bag) {
        reset(bag);
    }

    public ACollectionCursor(AOrderedList list) {
        reset(list);
    }

    @Override
    public boolean next() {
        pos++;
        if (pos < size) {
            object = values.get(pos);
            return true;
        } else {
            object = null;
            return false;
        }
    }

    @Override
    public IAObject get() { // returns the current object
        return object;
    }

    @Override
    public void reset() {
        pos = -1;
        size = values.size();
    }

    public void reset(AUnorderedList bag) {
        this.values = bag.values;
        reset();
    }

    public void reset(AOrderedList list) {
        this.values = list.values;
        reset();
    }

}