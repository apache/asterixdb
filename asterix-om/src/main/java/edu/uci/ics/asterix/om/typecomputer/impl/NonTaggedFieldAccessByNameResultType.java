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

import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.pointables.base.DefaultOpenFieldType;
import edu.uci.ics.asterix.om.typecomputer.base.IResultTypeComputer;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

public class NonTaggedFieldAccessByNameResultType implements IResultTypeComputer {

    public static final NonTaggedFieldAccessByNameResultType INSTANCE = new NonTaggedFieldAccessByNameResultType();

    private NonTaggedFieldAccessByNameResultType() {
    }

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expression;
        Object obj = env.getType(f.getArguments().get(0).getValue());

        if (obj == null) {
            return null;
        }
        IAType type0 = (IAType) obj;
        ARecordType t0 = getRecordTypeFromType(type0, expression);
        if (t0 == null) {
            return BuiltinType.ANY;
        }

        AbstractLogicalExpression arg1 = (AbstractLogicalExpression) f.getArguments().get(1).getValue();
        if (arg1.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
            return null; // BuiltinType.UNKNOWN;
        }

        ConstantExpression ce = (ConstantExpression) arg1;
        String typeName = ((AString) ((AsterixConstantValue) ce.getValue()).getObject()).getStringValue();
        for (int i = 0; i < t0.getFieldNames().length; i++) {
            if (t0.getFieldNames()[i].equals(typeName)) {
                return t0.getFieldTypes()[i];
            }
        }
        return BuiltinType.ANY;
    }

    static ARecordType getRecordTypeFromType(IAType type0, ILogicalExpression expression) throws AlgebricksException {
        switch (type0.getTypeTag()) {
            case RECORD: {
                return (ARecordType) type0;
            }
            case ANY: {
                return DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;
            }
            case UNION: {
                AUnionType u = (AUnionType) type0;
                if (u.isNullableType()) {
                    IAType t1 = u.getUnionList().get(1);
                    if (t1.getTypeTag() == ATypeTag.RECORD) {
                        return (ARecordType) t1;
                    }
                    if (t1.getTypeTag() == ATypeTag.ANY) {
                        return DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE;
                    }
                }
            }
            default: {
                throw new AlgebricksException("Unsupported type " + type0 + " for field access expression: "
                        + expression);
            }
        }

    }

}
