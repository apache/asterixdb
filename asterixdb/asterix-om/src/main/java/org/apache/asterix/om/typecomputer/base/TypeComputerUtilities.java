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

package org.apache.asterix.om.typecomputer.base;

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;

public class TypeComputerUtilities {

    public static boolean setRequiredAndInputTypes(AbstractFunctionCallExpression expr, IAType requiredRecordType,
            IAType inputRecordType) {
        boolean changed = false;
        Object[] opaqueParameters = expr.getOpaqueParameters();
        if (opaqueParameters == null) {
            opaqueParameters = new Object[2];
            opaqueParameters[0] = requiredRecordType;
            opaqueParameters[1] = inputRecordType;
            expr.setOpaqueParameters(opaqueParameters);
            changed = true;
        }
        return changed;
    }

    public static void resetRequiredAndInputTypes(AbstractFunctionCallExpression expr) {
        expr.setOpaqueParameters(null);
    }

    public static IAType getRequiredType(AbstractFunctionCallExpression expr) {
        Object[] type = expr.getOpaqueParameters();
        if (type != null) {
            IAType returnType = (IAType) type[0];
            return returnType;
        } else
            return null;
    }

    public static IAType getInputType(AbstractFunctionCallExpression expr) {
        Object[] type = expr.getOpaqueParameters();
        if (type != null) {
            IAType returnType = (IAType) type[1];
            return returnType;
        } else
            return null;
    }

    public static boolean inputInferednullableType(ILogicalExpression expression, IVariableTypeEnvironment env)
            throws AlgebricksException {
        AbstractFunctionCallExpression func = (AbstractFunctionCallExpression) expression;
        if (!(func instanceof ScalarFunctionCallExpression)) {
            return true;
        }
        List<Mutable<ILogicalExpression>> args = func.getArguments();
        for (Mutable<ILogicalExpression> arg : args) {
            IAType type = (IAType) env.getType(arg.getValue());
            if (type.getTypeTag() == ATypeTag.UNION || type.getTypeTag() == ATypeTag.NULL
                    || type.getTypeTag() == ATypeTag.ANY) {
                return true;
            }
            if (type.getTypeTag() == ATypeTag.RECORD || type.getTypeTag() == ATypeTag.UNORDEREDLIST
                    || type.getTypeTag() == ATypeTag.ORDEREDLIST) {
                if (nullableCompositeType(type)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean nullableCompositeType(IAType type) {
        if (type.getTypeTag() == ATypeTag.UNION || type.getTypeTag() == ATypeTag.NULL
                || type.getTypeTag() == ATypeTag.ANY) {
            return true;
        } else if (type.getTypeTag() == ATypeTag.RECORD) {
            ARecordType recordType = (ARecordType) type;
            IAType[] fieldTypes = recordType.getFieldTypes();
            for (IAType fieldType : fieldTypes) {
                boolean nullable = nullableCompositeType(fieldType);
                if (nullable) {
                    return true;
                }
            }
            return false;
        } else if (type.getTypeTag() == ATypeTag.UNORDEREDLIST || type.getTypeTag() == ATypeTag.ORDEREDLIST) {
            AbstractCollectionType collectionType = (AbstractCollectionType) type;
            IAType itemType = collectionType.getItemType();
            boolean nullable = nullableCompositeType(itemType);
            if (nullable) {
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }
}
