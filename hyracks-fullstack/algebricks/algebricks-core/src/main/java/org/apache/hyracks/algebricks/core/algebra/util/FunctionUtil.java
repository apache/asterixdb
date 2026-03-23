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

package org.apache.hyracks.algebricks.core.algebra.util;

import static org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions.AND;
import static org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions.EQ;
import static org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions.OR;

import java.util.BitSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class FunctionUtil {

    //TODO(wyk) add Multiply and Add
    private static final Set<FunctionIdentifier> COMMUTATIVE_FUNCTIONS = Set.of(EQ, AND, OR);

    /**
     * Compares two commutative expressions
     * TODO It doesn't support add and multiply (e.g., add(x, add(y, z) & add(add(x, y), z) will return false)
     *
     * @param expr1 left expression
     * @param expr2 right expression
     * @return true if equals, false otherwise
     */
    public static boolean commutativeEquals(ILogicalExpression expr1, ILogicalExpression expr2) {
        if (expr1.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL
                || expr2.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return expr1.equals(expr2);
        }

        AbstractFunctionCallExpression funcExpr1 = (AbstractFunctionCallExpression) expr1;
        AbstractFunctionCallExpression funcExpr2 = (AbstractFunctionCallExpression) expr2;

        FunctionIdentifier fid1 = funcExpr1.getFunctionIdentifier();
        FunctionIdentifier fid2 = funcExpr2.getFunctionIdentifier();

        if (!fid1.equals(fid2) || funcExpr1.getArguments().size() != funcExpr2.getArguments().size()) {
            return false;
        } else if (!COMMUTATIVE_FUNCTIONS.contains(fid1)) {
            return expr1.equals(expr2);
        }

        List<Mutable<ILogicalExpression>> args1 = funcExpr1.getArguments();
        List<Mutable<ILogicalExpression>> args2 = funcExpr2.getArguments();

        BitSet matched = new BitSet();
        int numberOfMatches = 0;
        for (Mutable<ILogicalExpression> arg1 : args1) {
            int prevNumberOfMatches = numberOfMatches;

            for (int i = 0; i < args2.size(); i++) {
                Mutable<ILogicalExpression> arg2 = args2.get(i);
                if (!matched.get(i) && commutativeEquals(arg1.getValue(), arg2.getValue())) {
                    matched.set(i);
                    numberOfMatches++;
                    break;
                }
            }

            if (numberOfMatches == prevNumberOfMatches) {
                // Early exit as one operand didn't match with any of the other operands
                return false;
            }
        }

        return numberOfMatches == args1.size();
    }

}
