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

import org.apache.asterix.om.typecomputer.base.AbstractResultTypeComputer;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.ConstantExpressionUtil;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;

public class FieldAccessByNameResultType extends AbstractResultTypeComputer {

    public static final FieldAccessByNameResultType INSTANCE = new FieldAccessByNameResultType();

    private FieldAccessByNameResultType() {
    }

    @Override
    protected void checkArgType(int argIndex, IAType type) throws AlgebricksException {
        if (argIndex == 0 && type.getTypeTag() != ATypeTag.RECORD) {
            throw new AlgebricksException("The first argument of a field access should be a RECORD, but it is " + type
                    + ".");
        }
        if (argIndex == 1 && type.getTypeTag() != ATypeTag.STRING) {
            throw new AlgebricksException("The second argument of a field access should be an STRING, but it is "
                    + type + ".");
        }
    }

    @Override
    protected IAType getResultType(ILogicalExpression expr, IAType... strippedInputTypes) throws AlgebricksException {
        IAType firstArgType = strippedInputTypes[0];
        if (firstArgType.getTypeTag() != ATypeTag.RECORD) {
            return BuiltinType.ANY;
        }
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        String fieldName = ConstantExpressionUtil.getStringArgument(funcExpr, 1);
        if (fieldName == null) {
            return BuiltinType.ANY;
        }
        ARecordType recType = (ARecordType) firstArgType;
        IAType fieldType = recType.getFieldType(fieldName);
        return fieldType == null ? BuiltinType.ANY : fieldType;
    }
}
