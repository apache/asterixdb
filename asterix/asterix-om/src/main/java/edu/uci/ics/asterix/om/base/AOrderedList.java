package edu.uci.ics.asterix.om.base;

import java.util.ArrayList;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.visitors.IOMVisitor;

public class AOrderedList implements IACollection {

    protected ArrayList<IAObject> values;
    protected AOrderedListType type;

    public AOrderedList(AOrderedListType type) {
        values = new ArrayList<IAObject>();
        this.type = type;
    }

    public AOrderedList(AOrderedListType type, ArrayList<IAObject> sequence) {
        values = sequence;
        this.type = type;
    }

    public void add(IAObject obj) {
        values.add(obj);
    }

    @Override
    public IACursor getCursor() {
        ACollectionCursor cursor = new ACollectionCursor();
        cursor.reset(this);
        return cursor;
    }

    @Override
    public IAType getType() {
        return type;
    }

    @Override
    public int size() {
        return values.size();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AOrderedList)) {
            return false;
        } else {
            AOrderedList y = (AOrderedList) o;
            return InMemUtils.cursorEquals(this.getCursor(), y.getCursor());
        }
    }

    @Override
    public int hashCode() {
        return InMemUtils.hashCursor(getCursor());
    }

    public IAObject getItem(int index) {
        return values.get(index);
    }

    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitAOrderedList(this);
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        return equals(obj);
    }

    @Override
    public int hash() {
        return hashCode();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("AOrderedList: [ ");
        boolean first = true;
        for (IAObject v : values) {
            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }
            sb.append(v.toString());
        }
        sb.append(" ]");
        return sb.toString();
    }
}