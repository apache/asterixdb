package edu.uci.ics.asterix.runtime.accessors.visitor;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.runtime.accessors.AFlatValueAccessor;
import edu.uci.ics.asterix.runtime.accessors.AListAccessor;
import edu.uci.ics.asterix.runtime.accessors.ARecordAccessor;

public interface IBinaryAccessorVisitor<R, T> {

    public R visit(AListAccessor accessor, T arg) throws AsterixException;

    public R visit(ARecordAccessor accessor, T arg) throws AsterixException;

    public R visit(AFlatValueAccessor accessor, T arg) throws AsterixException;
}
