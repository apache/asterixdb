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

import org.apache.asterix.om.typecomputer.base.IResultTypeComputer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.NonTaggedFormatUtil;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class NonTaggedNumericAddSubMulDivTypeComputer implements IResultTypeComputer {

    private static final String errMsg = "Arithmetic operations are not implemented for ";

    public static final NonTaggedNumericAddSubMulDivTypeComputer INSTANCE = new NonTaggedNumericAddSubMulDivTypeComputer();

    private NonTaggedNumericAddSubMulDivTypeComputer() {
    }

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expression;
        ILogicalExpression arg1 = fce.getArguments().get(0).getValue();
        ILogicalExpression arg2 = fce.getArguments().get(1).getValue();
        IAType t1;
        IAType t2;
        try {
            t1 = (IAType) env.getType(arg1);
            t2 = (IAType) env.getType(arg2);
        } catch (AlgebricksException e) {
            throw new AlgebricksException(e);
        }
        if (t1 == null || t2 == null) {
            return null;
        }

        ATypeTag tag1, tag2;
        if (NonTaggedFormatUtil.isOptional(t1))
            tag1 = ((AUnionType) t1).getNullableType().getTypeTag();
        else
            tag1 = t1.getTypeTag();

        if (NonTaggedFormatUtil.isOptional(t2))
            tag2 = ((AUnionType) t2).getNullableType().getTypeTag();
        else
            tag2 = t2.getTypeTag();

        if (tag1 == ATypeTag.NULL || tag2 == ATypeTag.NULL) {
            return BuiltinType.ANULL;
        }

        IAType type;

        switch (tag1) {
            case DOUBLE: {
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
                        return BuiltinType.ANY;
                    default: {
                        throw new NotImplementedException(errMsg + t2.getTypeName());
                    }
                }
                break;
            }
            case FLOAT: {
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
                        return BuiltinType.ANY;
                    default: {
                        throw new NotImplementedException(errMsg + t2.getTypeName());
                    }
                }
                break;
            }
            case INT64: {
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
                        return BuiltinType.ANY;
                    default: {
                        throw new NotImplementedException(errMsg + t2.getTypeName());
                    }
                }
                break;
            }
            case INT32: {
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
                        return BuiltinType.ANY;
                    default: {
                        throw new NotImplementedException(errMsg + tag2);
                    }
                }
                break;
            }
            case INT16: {
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
                        return BuiltinType.ANY;
                    default: {
                        throw new NotImplementedException(errMsg + tag2);
                    }
                }
                break;
            }
            case INT8: {
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
                        return BuiltinType.ANY;
                    default: {
                        throw new NotImplementedException(errMsg + tag2);
                    }
                }
                break;
            }
            case ANY: {
                switch (tag2) {
                    case INT8:
                    case INT16:
                    case INT32:
                    case INT64:
                    case FLOAT:
                    case ANY:
                    case DOUBLE:
                        return BuiltinType.ANY;
                    default: {
                        throw new NotImplementedException(errMsg + tag2);
                    }
                }
            }
            case DATE: {
                switch (tag2) {
                    case DATE:
                        type = BuiltinType.ADURATION;
                        break;
                    case YEARMONTHDURATION:
                    case DAYTIMEDURATION:
                    case DURATION:
                        type = BuiltinType.ADATE;
                        break;
                    default: {
                        throw new NotImplementedException(errMsg + tag2);
                    }
                }
                break;
            }
            case TIME: {
                switch (tag2) {
                    case TIME:
                        type = BuiltinType.ADURATION;
                        break;
                    case YEARMONTHDURATION:
                    case DAYTIMEDURATION:
                    case DURATION:
                        type = BuiltinType.ATIME;
                        break;
                    default: {
                        throw new NotImplementedException(errMsg + tag2);
                    }
                }
                break;
            }
            case DATETIME: {
                switch (tag2) {
                    case DATETIME:
                        type = BuiltinType.ADURATION;
                        break;
                    case YEARMONTHDURATION:
                    case DAYTIMEDURATION:
                    case DURATION:
                        type = BuiltinType.ADATETIME;
                        break;
                    default: {
                        throw new NotImplementedException(errMsg + tag2);
                    }
                }
                break;
            }
            case DURATION: {
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
                    default: {
                        throw new NotImplementedException(errMsg + tag2);
                    }
                }
                break;
            }
            case YEARMONTHDURATION: {
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
                    default: {
                        throw new NotImplementedException(errMsg + tag2);
                    }
                }
                break;
            }
            case DAYTIMEDURATION: {
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
                    default: {
                        throw new NotImplementedException(errMsg + tag2);
                    }
                }
                break;
            }
            default: {
                throw new NotImplementedException(errMsg + tag1);
            }
        }
        return AUnionType.createNullableType(type, "ArithemitcResult");
    }
}
