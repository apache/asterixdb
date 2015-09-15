/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.translator;

import org.apache.asterix.aql.base.Literal;
import org.apache.asterix.aql.literal.DoubleLiteral;
import org.apache.asterix.aql.literal.FloatLiteral;
import org.apache.asterix.aql.literal.IntegerLiteral;
import org.apache.asterix.aql.literal.LongIntegerLiteral;
import org.apache.asterix.aql.literal.StringLiteral;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.AFloat;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IAObject;

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
