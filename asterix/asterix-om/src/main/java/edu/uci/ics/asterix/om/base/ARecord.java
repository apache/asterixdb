package edu.uci.ics.asterix.om.base;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.visitors.IOMVisitor;

public class ARecord implements IAObject {

    protected ARecordType type;
    protected IAObject[] fields;

    public ARecord(ARecordType type, IAObject[] fields) {
        this.type = type;
        this.fields = fields;
    }

    @Override
    public void accept(IOMVisitor visitor) throws AsterixException {
        visitor.visitARecord(this);
    }

    @Override
    public ARecordType getType() {
        return type;
    }

    public boolean isOpen() {
        return type.isOpen();
    }

    // efficient way of retrieving the value of a field; pos starts from 0
    public IAObject getValueByPos(int pos) {
        return fields[pos];
    }

    public int numberOfFields() {
        return fields.length;
    }

    @Override
    public boolean deepEqual(IAObject obj) {
        if (!(obj instanceof ARecord)) {
            return false;
        }
        ARecord r = (ARecord) obj;
        if (!type.deepEqual(r.type)) {
            return false;
        }
        return InMemUtils.deepEqualArrays(fields, r.fields);
    }

    @Override
    public int hash() {
        int h = 0;
        for (int i = 0; i < fields.length; i++) {
            h += 31 * h + fields[i].hash();
        }
        return h;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ARecord: { ");
        for (int i = 0; i < fields.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(type.getFieldNames()[i]);
            sb.append(": ");
            sb.append(fields[i]);
        }
        sb.append(" }");
        return sb.toString();
    }
}
