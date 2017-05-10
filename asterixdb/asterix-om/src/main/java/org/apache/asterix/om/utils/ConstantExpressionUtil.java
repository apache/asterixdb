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
package org.apache.asterix.om.utils;

import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.AUnorderedList;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;

public class ConstantExpressionUtil {

    private ConstantExpressionUtil() {
    }

    public static IAObject getConstantIaObject(ILogicalExpression expr, ATypeTag typeTag) {
        if (expr.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
            return null;
        }
        final IAlgebricksConstantValue acv = ((ConstantExpression) expr).getValue();
        if (!(acv instanceof AsterixConstantValue)) {
            return null;
        }
        final IAObject iaObject = ((AsterixConstantValue) acv).getObject();
        if (typeTag != null) {
            return iaObject.getType().getTypeTag() == typeTag ? iaObject : null;
        } else {
            return iaObject;
        }
    }

    public static ATypeTag getConstantIaObjectType(ILogicalExpression expr) {
        IAObject iaObject = getConstantIaObject(expr, null);
        return iaObject.getType().getTypeTag();
    }

    public static Long getLongConstant(ILogicalExpression expr) {
        final IAObject iaObject = getConstantIaObject(expr, ATypeTag.BIGINT);
        return iaObject != null ? ((AInt64) iaObject).getLongValue() : null;
    }

    public static Integer getIntConstant(ILogicalExpression expr) {
        final IAObject iaObject = getConstantIaObject(expr, ATypeTag.INTEGER);
        return iaObject != null ? ((AInt32) iaObject).getIntegerValue() : null;
    }

    public static String getStringConstant(ILogicalExpression expr) {
        final IAObject iaObject = getConstantIaObject(expr, ATypeTag.STRING);
        return iaObject != null ? ((AString) iaObject).getStringValue() : null;
    }

    public static String getStringConstant(IAObject iaObject) {
        // Make sure to call this method after checking the type of the given object.
        return iaObject != null ? ((AString) iaObject).getStringValue() : null;
    }

    public static AOrderedList getOrderedListConstant(IAObject iaObject) {
        // Make sure to call this method after checking the type of the given object.
        return iaObject != null ? (AOrderedList) iaObject : null;
    }

    public static AUnorderedList getUnorderedListConstant(IAObject iaObject) {
        // Make sure to call this method after checking the type of the given object.
        return iaObject != null ? (AUnorderedList) iaObject : null;
    }

    public static Boolean getBooleanConstant(ILogicalExpression expr) {
        final IAObject iaObject = getConstantIaObject(expr, ATypeTag.BOOLEAN);
        return iaObject != null ? ((ABoolean) iaObject).getBoolean() : null;
    }

    public static Integer getIntArgument(AbstractFunctionCallExpression f, int index) {
        return getIntConstant(f.getArguments().get(index).getValue());
    }

    public static String getStringArgument(AbstractFunctionCallExpression f, int index) {
        return getStringConstant(f.getArguments().get(index).getValue());
    }

    public static Integer getIntArgument(ILogicalExpression expr, int index) {
        return expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL
                ? getIntArgument((AbstractFunctionCallExpression) expr, index) : null;
    }

    public static String getStringArgument(ILogicalExpression expr, int index) {
        return expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL
                ? getStringArgument((AbstractFunctionCallExpression) expr, index) : null;
    }
}
