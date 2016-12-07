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
package org.apache.asterix.om.typecomputer.impl;

import org.apache.asterix.om.exceptions.IncompatibleTypeException;
import org.apache.asterix.om.typecomputer.base.AbstractResultTypeComputer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;

public class NumericAddSubMulDivTypeComputer extends AbstractResultTypeComputer {

    public static final NumericAddSubMulDivTypeComputer INSTANCE = new NumericAddSubMulDivTypeComputer();

    private NumericAddSubMulDivTypeComputer() {
    }

    @Override
    protected IAType getResultType(ILogicalExpression expr, IAType... strippedInputTypes) throws AlgebricksException {
        AbstractFunctionCallExpression functionCallExpression = (AbstractFunctionCallExpression) expr;
        String funcName = functionCallExpression.getFunctionIdentifier().getName();
        IAType t1 = strippedInputTypes[0];
        IAType t2 = strippedInputTypes[1];
        ATypeTag tag1 = t1.getTypeTag();
        ATypeTag tag2 = t2.getTypeTag();

        IAType type;
        switch (tag1) {
            case DOUBLE:
                switch (tag2) {
                    case INT8:
                    case INT16:
                    case INT32:
                    case INT64:
                    case FLOAT:
                    case DOUBLE:
                        type = BuiltinType.ADOUBLE;
                        break;
                    case ANY:
                        type = BuiltinType.ANY;
                        break;
                    default:
                        throw new IncompatibleTypeException(funcName, tag1, tag2);
                }
                break;
            case FLOAT:
                switch (tag2) {
                    case INT8:
                    case INT16:
                    case INT32:
                    case INT64:
                    case FLOAT:
                        type = BuiltinType.AFLOAT;
                        break;
                    case DOUBLE:
                        type = BuiltinType.ADOUBLE;
                        break;
                    case ANY:
                        type = BuiltinType.ANY;
                        break;
                    default:
                        throw new IncompatibleTypeException(funcName, tag1, tag2);
                }
                break;
            case INT64:
                switch (tag2) {
                    case INT8:
                    case INT16:
                    case INT32:
                    case INT64:
                        type = BuiltinType.AINT64;
                        break;
                    case FLOAT:
                        type = BuiltinType.AFLOAT;
                        break;
                    case DOUBLE:
                        type = BuiltinType.ADOUBLE;
                        break;
                    case ANY:
                        type = BuiltinType.ANY;
                        break;
                    default:
                        throw new IncompatibleTypeException(funcName, tag1, tag2);
                }
                break;
            case INT32:
                switch (tag2) {
                    case INT8:
                    case INT16:
                    case INT32:
                        type = BuiltinType.AINT32;
                        break;
                    case INT64:
                        type = BuiltinType.AINT64;
                        break;
                    case FLOAT:
                        type = BuiltinType.AFLOAT;
                        break;
                    case DOUBLE:
                        type = BuiltinType.ADOUBLE;
                        break;
                    case ANY:
                        type = BuiltinType.ANY;
                        break;
                    default:
                        throw new IncompatibleTypeException(funcName, tag1, tag2);
                }
                break;
            case INT16:
                switch (tag2) {
                    case INT8:
                    case INT16:
                        type = BuiltinType.AINT16;
                        break;
                    case INT32:
                        type = BuiltinType.AINT32;
                        break;
                    case INT64:
                        type = BuiltinType.AINT64;
                        break;
                    case FLOAT:
                        type = BuiltinType.AFLOAT;
                        break;
                    case DOUBLE:
                        type = BuiltinType.ADOUBLE;
                        break;
                    case ANY:
                        type = BuiltinType.ANY;
                        break;
                    default:
                        throw new IncompatibleTypeException(funcName, tag1, tag2);
                }
                break;
            case INT8:
                switch (tag2) {
                    case INT8:
                        type = BuiltinType.AINT8;
                        break;
                    case INT16:
                        type = BuiltinType.AINT16;
                        break;
                    case INT32:
                        type = BuiltinType.AINT32;
                        break;
                    case INT64:
                        type = BuiltinType.AINT64;
                        break;
                    case FLOAT:
                        type = BuiltinType.AFLOAT;
                        break;
                    case DOUBLE:
                        type = BuiltinType.ADOUBLE;
                        break;
                    case ANY:
                        type = BuiltinType.ANY;
                        break;
                    default:
                        throw new IncompatibleTypeException(funcName, tag1, tag2);
                }
                break;
            case ANY:
                switch (tag2) {
                    case INT8:
                    case INT16:
                    case INT32:
                    case INT64:
                    case FLOAT:
                    case ANY:
                    case DOUBLE:
                        type = BuiltinType.ANY;
                        break;
                    default:
                        throw new IncompatibleTypeException(funcName, tag1, tag2);
                }
                break;
            case DATE:
                switch (tag2) {
                    case DATE:
                        type = BuiltinType.ADURATION;
                        break;
                    case YEARMONTHDURATION:
                    case DAYTIMEDURATION:
                    case DURATION:
                        type = BuiltinType.ADATE;
                        break;
                    case ANY:
                        type = BuiltinType.ANY;
                        break;
                    default:
                        throw new IncompatibleTypeException(funcName, tag1, tag2);
                }
                break;
            case TIME:
                switch (tag2) {
                    case TIME:
                        type = BuiltinType.ADURATION;
                        break;
                    case YEARMONTHDURATION:
                    case DAYTIMEDURATION:
                    case DURATION:
                        type = BuiltinType.ATIME;
                        break;
                    case ANY:
                        type = BuiltinType.ANY;
                        break;
                    default:
                        throw new IncompatibleTypeException(funcName, tag1, tag2);
                }
                break;
            case DATETIME:
                switch (tag2) {
                    case DATETIME:
                        type = BuiltinType.ADURATION;
                        break;
                    case YEARMONTHDURATION:
                    case DAYTIMEDURATION:
                    case DURATION:
                        type = BuiltinType.ADATETIME;
                        break;
                    default:
                        throw new IncompatibleTypeException(funcName, tag1, tag2);
                }
                break;
            case DURATION:
                switch (tag2) {
                    case DATE:
                        type = BuiltinType.ADATE;
                        break;
                    case TIME:
                        type = BuiltinType.ATIME;
                        break;
                    case DATETIME:
                        type = BuiltinType.ADATETIME;
                        break;
                    case ANY:
                        type = BuiltinType.ANY;
                        break;
                    default:
                        throw new IncompatibleTypeException(funcName, tag1, tag2);
                }
                break;
            case YEARMONTHDURATION:
                switch (tag2) {
                    case DATE:
                        type = BuiltinType.ADATE;
                        break;
                    case TIME:
                        type = BuiltinType.ATIME;
                        break;
                    case DATETIME:
                        type = BuiltinType.ADATETIME;
                        break;
                    case YEARMONTHDURATION:
                        type = BuiltinType.AYEARMONTHDURATION;
                        break;
                    case ANY:
                        type = BuiltinType.ANY;
                        break;
                    default:
                        throw new IncompatibleTypeException(funcName, tag1, tag2);
                }
                break;
            case DAYTIMEDURATION:
                switch (tag2) {
                    case DATE:
                        type = BuiltinType.ADATE;
                        break;
                    case TIME:
                        type = BuiltinType.ATIME;
                        break;
                    case DATETIME:
                        type = BuiltinType.ADATETIME;
                        break;
                    case DAYTIMEDURATION:
                        type = BuiltinType.ADAYTIMEDURATION;
                        break;
                    case ANY:
                        type = BuiltinType.ANY;
                        break;
                    default:
                        throw new IncompatibleTypeException(funcName, tag1, tag2);
                }
                break;
            default:
                throw new IncompatibleTypeException(funcName, tag1, tag2);
        }
        return type;
    }
}
