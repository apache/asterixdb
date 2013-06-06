/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.translator;

import edu.uci.ics.asterix.aql.base.Literal;
import edu.uci.ics.asterix.aql.literal.DoubleLiteral;
import edu.uci.ics.asterix.aql.literal.FloatLiteral;
import edu.uci.ics.asterix.aql.literal.IntegerLiteral;
import edu.uci.ics.asterix.aql.literal.LongIntegerLiteral;
import edu.uci.ics.asterix.aql.literal.StringLiteral;
import edu.uci.ics.asterix.om.base.ABoolean;
import edu.uci.ics.asterix.om.base.ADouble;
import edu.uci.ics.asterix.om.base.AFloat;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AInt64;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.base.IAObject;

public final class ConstantHelper {

    public static IAObject objectFromLiteral(Literal valLiteral) {
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
            case LONG: {
                LongIntegerLiteral il = (LongIntegerLiteral) valLiteral;
                return new AInt64(il.getValue());                
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
