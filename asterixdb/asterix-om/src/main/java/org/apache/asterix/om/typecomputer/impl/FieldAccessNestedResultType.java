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

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.exceptions.TypeMismatchException;
import org.apache.asterix.om.exceptions.UnsupportedItemTypeException;
import org.apache.asterix.om.typecomputer.base.AbstractResultTypeComputer;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;

public class FieldAccessNestedResultType extends AbstractResultTypeComputer {
    public static final FieldAccessNestedResultType INSTANCE = new FieldAccessNestedResultType();

    private FieldAccessNestedResultType() {
    }

    @Override
    protected void checkArgType(String funcName, int argIndex, IAType type) throws AlgebricksException {
        ATypeTag actualTypeTag = type.getTypeTag();
        if (argIndex == 0 && actualTypeTag != ATypeTag.RECORD) {
            throw new TypeMismatchException(funcName, argIndex, actualTypeTag, ATypeTag.RECORD);
        }
        if (argIndex == 1) {
            switch (actualTypeTag) {
                case STRING:
                    break;
                case ORDEREDLIST:
                    checkOrderedList(funcName, type);
                    break;
                default:
                    throw new TypeMismatchException(funcName, argIndex, actualTypeTag, ATypeTag.STRING,
                            ATypeTag.ORDEREDLIST);
            }
        }
    }

    private void checkOrderedList(String funcName, IAType type) throws AlgebricksException {
        AOrderedListType listType = (AOrderedListType) type;
        ATypeTag itemTypeTag = listType.getItemType().getTypeTag();
        if (itemTypeTag != ATypeTag.STRING && itemTypeTag != ATypeTag.ANY) {
            throw new UnsupportedItemTypeException(funcName, itemTypeTag);
        }
    }

    @Override
    protected IAType getResultType(ILogicalExpression expr, IAType... strippedInputTypes) throws AlgebricksException {
        IAType firstArgType = strippedInputTypes[0];
        if (firstArgType.getTypeTag() != ATypeTag.RECORD) {
            return BuiltinType.ANY;
        }
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        ILogicalExpression arg1 = funcExpr.getArguments().get(1).getValue();
        if (arg1.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
            return BuiltinType.ANY;
        }
        ConstantExpression ce = (ConstantExpression) arg1;
        IAObject v = ((AsterixConstantValue) ce.getValue()).getObject();
        List<String> fieldPath = new ArrayList<>();
        if (v.getType().getTypeTag() == ATypeTag.ORDEREDLIST) {
            for (int i = 0; i < ((AOrderedList) v).size(); i++) {
                fieldPath.add(((AString) ((AOrderedList) v).getItem(i)).getStringValue());
            }
        } else {
            fieldPath.add(((AString) v).getStringValue());
        }
        ARecordType recType = (ARecordType) firstArgType;
        IAType fieldType = recType.getSubFieldType(fieldPath);
        return fieldType == null ? BuiltinType.ANY : fieldType;
    }

}
