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
package edu.uci.ics.asterix.om.typecomputer.impl;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.asterix.om.typecomputer.base.IResultTypeComputer;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

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
        if (t1.getTypeTag() == ATypeTag.UNION && NonTaggedFormatUtil.isOptionalField((AUnionType) t1))
            tag1 = ((AUnionType) t1).getUnionList().get(NonTaggedFormatUtil.OPTIONAL_TYPE_INDEX_IN_UNION_LIST)
                    .getTypeTag();
        else
            tag1 = t1.getTypeTag();

        if (t2.getTypeTag() == ATypeTag.UNION && NonTaggedFormatUtil.isOptionalField((AUnionType) t2))
            tag2 = ((AUnionType) t2).getUnionList().get(NonTaggedFormatUtil.OPTIONAL_TYPE_INDEX_IN_UNION_LIST)
                    .getTypeTag();
        else
            tag2 = t2.getTypeTag();

        if (tag1 == ATypeTag.NULL || tag2 == ATypeTag.NULL) {
            return BuiltinType.ANULL;
        }

        List<IAType> unionList = new ArrayList<IAType>();
        unionList.add(BuiltinType.ANULL);

        switch (tag1) {
            case DOUBLE: {
                switch (tag2) {
                    case INT8:
                    case INT16:
                    case INT32:
                    case INT64:
                    case FLOAT:
                    case DOUBLE:
                        unionList.add(BuiltinType.ADOUBLE);
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
                        unionList.add(BuiltinType.AFLOAT);
                        break;
                    case DOUBLE:
                        unionList.add(BuiltinType.ADOUBLE);
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
                        unionList.add(BuiltinType.AINT64);
                        break;
                    case FLOAT:
                        unionList.add(BuiltinType.AFLOAT);
                        break;
                    case DOUBLE:
                        unionList.add(BuiltinType.ADOUBLE);
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
                        unionList.add(BuiltinType.AINT32);
                        break;
                    case INT64:
                        unionList.add(BuiltinType.AINT64);
                        break;
                    case FLOAT:
                        unionList.add(BuiltinType.AFLOAT);
                        break;
                    case DOUBLE:
                        unionList.add(BuiltinType.ADOUBLE);
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
                        unionList.add(BuiltinType.AINT16);
                        break;
                    case INT32:
                        unionList.add(BuiltinType.AINT32);
                        break;
                    case INT64:
                        unionList.add(BuiltinType.AINT64);
                        break;
                    case FLOAT:
                        unionList.add(BuiltinType.AFLOAT);
                        break;
                    case DOUBLE:
                        unionList.add(BuiltinType.ADOUBLE);
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
                        unionList.add(BuiltinType.AINT8);
                        break;
                    case INT16:
                        unionList.add(BuiltinType.AINT16);
                        break;
                    case INT32:
                        unionList.add(BuiltinType.AINT32);
                        break;
                    case INT64:
                        unionList.add(BuiltinType.AINT64);
                        break;
                    case FLOAT:
                        unionList.add(BuiltinType.AFLOAT);
                        break;
                    case DOUBLE:
                        unionList.add(BuiltinType.ADOUBLE);
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
                        unionList.add(BuiltinType.ADURATION);
                        break;
                    case YEARMONTHDURATION:
                    case DAYTIMEDURATION:
                    case DURATION:
                        unionList.add(BuiltinType.ADATE);
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
                        unionList.add(BuiltinType.ADURATION);
                        break;
                    case YEARMONTHDURATION:
                    case DAYTIMEDURATION:
                    case DURATION:
                        unionList.add(BuiltinType.ATIME);
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
                        unionList.add(BuiltinType.ADURATION);
                        break;
                    case YEARMONTHDURATION:
                    case DAYTIMEDURATION:
                    case DURATION:
                        unionList.add(BuiltinType.ADATETIME);
                        break;
                    default: {
                        throw new NotImplementedException(errMsg + tag2);
                    }
                }
                break;
            }
            case DURATION: {
                switch(tag2){
                    case DATE:
                        unionList.add(BuiltinType.ADATE);
                        break;
                    case TIME:
                        unionList.add(BuiltinType.ATIME);
                        break;
                    case DATETIME:
                        unionList.add(BuiltinType.ADATETIME);
                        break;
                    default: {
                        throw new NotImplementedException(errMsg + tag2);
                    }
                }
                break;
            }
            case YEARMONTHDURATION: {
                switch(tag2){
                    case DATE:
                        unionList.add(BuiltinType.ADATE);
                        break;
                    case TIME:
                        unionList.add(BuiltinType.ATIME);
                        break;
                    case DATETIME:
                        unionList.add(BuiltinType.ADATETIME);
                        break;
                    case YEARMONTHDURATION:
                        unionList.add(BuiltinType.AYEARMONTHDURATION);
                        break;
                    default: {
                        throw new NotImplementedException(errMsg + tag2);
                    }
                }
                break;
            }
            case DAYTIMEDURATION: {
                switch(tag2){
                    case DATE:
                        unionList.add(BuiltinType.ADATE);
                        break;
                    case TIME:
                        unionList.add(BuiltinType.ATIME);
                        break;
                    case DATETIME:
                        unionList.add(BuiltinType.ADATETIME);
                        break;
                    case DAYTIMEDURATION:
                        unionList.add(BuiltinType.ADAYTIMEDURATION);
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
        return new AUnionType(unionList, "ArithemitcResult");
    }
}
