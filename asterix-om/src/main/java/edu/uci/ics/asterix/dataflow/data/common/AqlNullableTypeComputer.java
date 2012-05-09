package edu.uci.ics.asterix.dataflow.data.common;


import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.types.TypeHelper;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.INullableTypeComputer;

public class AqlNullableTypeComputer implements INullableTypeComputer {

    public static final AqlNullableTypeComputer INSTANCE = new AqlNullableTypeComputer();

    private AqlNullableTypeComputer() {
    }

    @Override
    public IAType makeNullableType(Object type) throws AlgebricksException {
        IAType t = (IAType) type;
        if (TypeHelper.canBeNull(t)) {
            return t;
        } else {
            return AUnionType.createNullableType(t);
        }
    }

}
