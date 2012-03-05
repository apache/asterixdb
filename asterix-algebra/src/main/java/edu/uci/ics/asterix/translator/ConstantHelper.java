package edu.uci.ics.asterix.translator;

import edu.uci.ics.asterix.aql.base.ILiteral;
import edu.uci.ics.asterix.aql.literal.DoubleLiteral;
import edu.uci.ics.asterix.aql.literal.FloatLiteral;
import edu.uci.ics.asterix.aql.literal.IntegerLiteral;
import edu.uci.ics.asterix.aql.literal.StringLiteral;
import edu.uci.ics.asterix.om.base.ABoolean;
import edu.uci.ics.asterix.om.base.ADouble;
import edu.uci.ics.asterix.om.base.AFloat;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.base.IAObject;

public final class ConstantHelper {

    public static IAObject objectFromLiteral(ILiteral valLiteral) {
        switch (valLiteral.getLiteralType()) {
            case DOUBLE: {
                DoubleLiteral d = (DoubleLiteral) valLiteral;
                return new ADouble(d.getValue());
            }
            case FALSE: {
                return ABoolean.FALSE;
            }
            case FLOAT: {
                FloatLiteral fl = (FloatLiteral) valLiteral;
                return new AFloat(fl.getValue());
            }
            case INTEGER: {
                IntegerLiteral il = (IntegerLiteral) valLiteral;
                return new AInt32(il.getValue());
            }
            case NULL: {
                return ANull.NULL;
            }
            case STRING: {
                StringLiteral sl = (StringLiteral) valLiteral;
                return new AString(sl.getValue());
            }
            case TRUE: {
                return ABoolean.TRUE;
            }
            default: {
                throw new IllegalStateException();
            }
        }
    }

}
