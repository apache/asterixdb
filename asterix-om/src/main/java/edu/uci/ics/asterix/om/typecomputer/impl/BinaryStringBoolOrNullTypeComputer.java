package edu.uci.ics.asterix.om.typecomputer.impl;

import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.types.TypeHelper;

/**
 *
 * @author Xiaoyu Ma
 */
public class BinaryStringBoolOrNullTypeComputer extends AbstractBinaryStringTypeComputer {
    public static final BinaryStringBoolOrNullTypeComputer INSTANCE = new BinaryStringBoolOrNullTypeComputer();
    private BinaryStringBoolOrNullTypeComputer() {}    
    
    @Override
    public IAType getResultType(IAType t0, IAType t1) {
        if (TypeHelper.canBeNull(t0) || TypeHelper.canBeNull(t1)) {
            return AUnionType.createNullableType(BuiltinType.ABOOLEAN);
        }        	
        return BuiltinType.ABOOLEAN;
    }    
}
