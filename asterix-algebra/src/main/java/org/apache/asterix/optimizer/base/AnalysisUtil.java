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
package org.apache.asterix.optimizer.base;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.metadata.declared.AqlSourceId;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractDataSourceOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;

public class AnalysisUtil {
    /*
     * If the first child of op is of type opType, then it returns that child,
     * o/w returns null.
     */
    public final static ILogicalOperator firstChildOfType(AbstractLogicalOperator op, LogicalOperatorTag opType) {
        List<Mutable<ILogicalOperator>> ins = op.getInputs();
        if (ins == null || ins.isEmpty()) {
            return null;
        }
        Mutable<ILogicalOperator> opRef2 = ins.get(0);
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) opRef2.getValue();
        if (op2.getOperatorTag() == opType) {
            return op2;
        } else {
            return null;
        }
    }

    public static int numberOfVarsInExpr(ILogicalExpression e) {
        switch (((AbstractLogicalExpression) e).getExpressionTag()) {
            case CONSTANT: {
                return 0;
            }
            case FUNCTION_CALL: {
                AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) e;
                int s = 0;
                for (Mutable<ILogicalExpression> arg : f.getArguments()) {
                    s += numberOfVarsInExpr(arg.getValue());
                }
                return s;
            }
            case VARIABLE: {
                return 1;
            }
            default: {
                assert false;
                throw new IllegalArgumentException();
            }
        }
    }

    public static boolean isRunnableFieldAccessFunction(FunctionIdentifier fid) {
        return fieldAccessFunctions.contains(fid);
    }

    public static boolean isDataSetCall(ILogicalExpression e) {
        if (((AbstractLogicalExpression) e).getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression fe = (AbstractFunctionCallExpression) e;
        return AsterixBuiltinFunctions.isDatasetFunction(fe.getFunctionIdentifier());
    }

    public static boolean isRunnableAccessToFieldRecord(ILogicalExpression expr) {
        if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression fc = (AbstractFunctionCallExpression) expr;
            FunctionIdentifier fid = fc.getFunctionIdentifier();
            if (AnalysisUtil.isRunnableFieldAccessFunction(fid)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isAccessByNameToFieldRecord(ILogicalExpression expr) {
        if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression fc = (AbstractFunctionCallExpression) expr;
            FunctionIdentifier fid = fc.getFunctionIdentifier();
            if (fid.equals(AsterixBuiltinFunctions.FIELD_ACCESS_BY_NAME)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isAccessToFieldRecord(ILogicalExpression expr) {
        if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression fc = (AbstractFunctionCallExpression) expr;
            FunctionIdentifier fid = fc.getFunctionIdentifier();
            if (fid.equals(AsterixBuiltinFunctions.FIELD_ACCESS_BY_INDEX)
                    || fid.equals(AsterixBuiltinFunctions.FIELD_ACCESS_BY_NAME)
                    || fid.equals(AsterixBuiltinFunctions.FIELD_ACCESS_NESTED)) {
                return true;
            }
        }
        return false;
    }

    public static Pair<String, String> getDatasetInfo(AbstractDataSourceOperator op) throws AlgebricksException {
        AqlSourceId srcId = (AqlSourceId) op.getDataSource().getId();
        return new Pair<String, String>(srcId.getDataverseName(), srcId.getDatasourceName());
    }

    private static List<FunctionIdentifier> fieldAccessFunctions = new ArrayList<FunctionIdentifier>();

    static {
        fieldAccessFunctions.add(AsterixBuiltinFunctions.GET_DATA);
        fieldAccessFunctions.add(AsterixBuiltinFunctions.GET_HANDLE);
        fieldAccessFunctions.add(AsterixBuiltinFunctions.TYPE_OF);
    }

}
