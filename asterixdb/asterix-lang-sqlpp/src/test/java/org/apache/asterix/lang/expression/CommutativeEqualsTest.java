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
package org.apache.asterix.lang.expression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.junit.Assert;
import org.junit.Test;

/**
 * TODO include multiply and add when supported
 *
 * @see FunctionUtil#commutativeEquals(ILogicalExpression, ILogicalExpression)
 */
public class CommutativeEqualsTest {
    private final Map<Character, LogicalVariable> varNameToVarMap = new HashMap<>();
    private int varCounter;

    @Test
    public void testTwoOperands() {
        // EQ
        reset();
        ILogicalExpression expr1 = createExpression(BuiltinFunctions.EQ, 'x', 'y');
        ILogicalExpression expr2 = createExpression(BuiltinFunctions.EQ, 'x', 'y');
        Assert.assertTrue(FunctionUtil.commutativeEquals(expr1, expr2));

        reset();
        expr1 = createExpression(BuiltinFunctions.EQ, 'x', 'y');
        expr2 = createExpression(BuiltinFunctions.EQ, 'y', 'x');
        Assert.assertTrue(FunctionUtil.commutativeEquals(expr1, expr2));

        reset();
        expr1 = createExpression(BuiltinFunctions.EQ, 'x', 'x');
        expr2 = createExpression(BuiltinFunctions.EQ, 'x', 'y');
        Assert.assertFalse(FunctionUtil.commutativeEquals(expr1, expr2));
    }

    private void reset() {
        varCounter = 0;
        varNameToVarMap.clear();
    }

    private ILogicalExpression createExpression(FunctionIdentifier fid, char left, char right) {
        List<Mutable<ILogicalExpression>> args = new ArrayList<>();

        args.add(getVariableExpression(left));
        args.add(getVariableExpression(right));

        IFunctionInfo funcInfo = BuiltinFunctions.getBuiltinFunctionInfo(fid);
        return new ScalarFunctionCallExpression(funcInfo, args);
    }

    private Mutable<ILogicalExpression> getVariableExpression(Character displayName) {
        LogicalVariable variable = varNameToVarMap.computeIfAbsent(displayName,
                k -> new LogicalVariable(varCounter++, displayName.toString()));
        return new MutableObject<>(new VariableReferenceExpression(variable));
    }
}
