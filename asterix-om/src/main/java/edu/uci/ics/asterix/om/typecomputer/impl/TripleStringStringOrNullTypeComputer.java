package edu.uci.ics.asterix.om.typecomputer.impl;

import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.types.TypeHelper;

/**
 *
 * @author Xiaoyu Ma
 */
public class TripleStringStringOrNullTypeComputer  extends AbstractTripleStringTypeComputer {
    public static final TripleStringStringOrNullTypeComputer INSTANCE = new TripleStringStringOrNullTypeComputer();
    private TripleStringStringOrNullTypeComputer() {}

    @Override
    public IAType getResultType(IAType t0, IAType t1, IAType t2) {
        if (TypeHelper.canBeNull(t0) || TypeHelper.canBeNull(t1) || TypeHelper.canBeNull(t2)) {
            return AUnionType.createNullableType(BuiltinType.ASTRING);
        }      	
        return BuiltinType.ASTRING;
    } 
}
