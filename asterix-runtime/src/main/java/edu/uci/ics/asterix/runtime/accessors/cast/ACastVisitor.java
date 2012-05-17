package edu.uci.ics.asterix.runtime.accessors.cast;

import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.runtime.accessors.AFlatValueAccessor;
import edu.uci.ics.asterix.runtime.accessors.AListAccessor;
import edu.uci.ics.asterix.runtime.accessors.ARecordAccessor;
import edu.uci.ics.asterix.runtime.accessors.visitor.IBinaryAccessorVisitor;
import edu.uci.ics.hyracks.algebricks.common.utils.Triple;

public class ACastVisitor implements IBinaryAccessorVisitor<Void, Triple<ARecordAccessor, IAType, Boolean>> {

    private Map<ARecordAccessor, ARecordCaster> raccessorToCaster = new HashMap<ARecordAccessor, ARecordCaster>();

    @Override
    public Void visit(AListAccessor accessor, Triple<ARecordAccessor, IAType, Boolean> arg) {
        return null;
    }

    @Override
    public Void visit(ARecordAccessor accessor, Triple<ARecordAccessor, IAType, Boolean> arg) throws AsterixException {
        ARecordCaster caster = raccessorToCaster.get(accessor);
        if (caster == null) {
            caster = new ARecordCaster();
            raccessorToCaster.put(accessor, caster);
        }
        try {
            caster.castRecord(accessor, arg.first, (ARecordType) arg.second, this);
        } catch (Exception e) {
            throw new AsterixException(e);
        }
        return null;
    }

    @Override
    public Void visit(AFlatValueAccessor accessor, Triple<ARecordAccessor, IAType, Boolean> arg) {
        return null;
    }

}
