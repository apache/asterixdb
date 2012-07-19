package edu.uci.ics.asterix.om.typecomputer.impl;

import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.types.TypeHelper;

/**
 *
 * @author Xiaoyu Ma
 */
public class QuadStringStringOrNullTypeComputer  extends AbstractQuadStringTypeComputer {
    public static final QuadStringStringOrNullTypeComputer INSTANCE = new QuadStringStringOrNullTypeComputer();
    private QuadStringStringOrNullTypeComputer() {}

    @Override
    public IAType getResultType(IAType t0, IAType t1, IAType t2, IAType t3) {
        if (TypeHelper.canBeNull(t0) || TypeHelper.canBeNull(t1) || 
        	TypeHelper.canBeNull(t2) || TypeHelper.canBeNull(t3)) {
            return AUnionType.createNullableType(BuiltinType.ASTRING);
        }      	
        return BuiltinType.ASTRING;
    }
    
}
