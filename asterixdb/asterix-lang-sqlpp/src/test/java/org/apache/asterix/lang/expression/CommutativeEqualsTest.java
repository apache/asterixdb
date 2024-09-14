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
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
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
    public void testExpressions() {
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

        // ( ( NOT ( bool_field1 ) ) ) AND ( ( bool_field1 ) )
        // ( ( bool_field1 ) ) AND ( ( bool_field1 = true ) )
        reset();
        expr1 = createExpression(BuiltinFunctions.AND,
                createExpression(BuiltinFunctions.NOT, getVariableExpression('x')), getVariableExpression('x'));

        expr2 = createExpression(BuiltinFunctions.AND, getVariableExpression('x'),
                createExpression(BuiltinFunctions.EQ, getVariableExpression('x'), ConstantExpression.TRUE));
        Assert.assertFalse(FunctionUtil.commutativeEquals(expr1, expr2));

        //  ( UPPER(varchar_field1) = varchar_field1 AND int_field1 = 2 )
        //  ( LOWER(varchar_field1) = varchar_field1 AND int_field1 = 2 )
        reset();
        expr1 = createExpression(BuiltinFunctions.AND,
                createExpression(BuiltinFunctions.EQ,
                        createExpression(BuiltinFunctions.STRING_UPPERCASE, getVariableExpression('x')),
                        ConstantExpression.TRUE),
                createExpression(BuiltinFunctions.EQ, getVariableExpression('y'),
                        new ConstantExpression(new AsterixConstantValue(new AInt32(2)))));

        expr2 = createExpression(BuiltinFunctions.AND,
                createExpression(BuiltinFunctions.EQ,
                        createExpression(BuiltinFunctions.STRING_LOWERCASE, getVariableExpression('x')),
                        ConstantExpression.TRUE),
                createExpression(BuiltinFunctions.EQ, getVariableExpression('y'),
                        new ConstantExpression(new AsterixConstantValue(new AInt32(2)))));

        //  ( LOWER(varchar_field1) = varchar_field1 AND int_field1 = 2 )
        //  ( int_field1 = 2 AND LOWER(varchar_field1) = varchar_field1)
        // should evaluate to true
        ILogicalExpression expr3 = createExpression(BuiltinFunctions.AND,
                createExpression(BuiltinFunctions.EQ, getVariableExpression('y'),
                        new ConstantExpression(new AsterixConstantValue(new AInt32(2)))),
                createExpression(BuiltinFunctions.EQ,
                        createExpression(BuiltinFunctions.STRING_LOWERCASE, getVariableExpression('x')),
                        ConstantExpression.TRUE));

        Assert.assertFalse(FunctionUtil.commutativeEquals(expr1, expr2));
        Assert.assertTrue(FunctionUtil.commutativeEquals(expr2, expr3));

        // ( 10/int_field1=6911432 ) AND ( bool_field1=true )
        // ( int_field1/10=6911432  )  AND ( bool_field1 = true )
        reset();
        expr1 = createExpression(BuiltinFunctions.AND,
                createExpression(BuiltinFunctions.NUMERIC_DIV,
                        new ConstantExpression(new AsterixConstantValue(new AInt32(10))), getVariableExpression('i')),
                createExpression(BuiltinFunctions.EQ, getVariableExpression('b'), ConstantExpression.TRUE));

        expr2 = createExpression(BuiltinFunctions.AND,
                createExpression(BuiltinFunctions.NUMERIC_DIV, getVariableExpression('i'),
                        new ConstantExpression(new AsterixConstantValue(new AInt32(10)))),
                createExpression(BuiltinFunctions.EQ, getVariableExpression('b'), ConstantExpression.TRUE));

        Assert.assertFalse(FunctionUtil.commutativeEquals(expr1, expr2));
    }

    private void reset() {
        varCounter = 0;
        varNameToVarMap.clear();
    }

    private ILogicalExpression createExpression(FunctionIdentifier fid, ILogicalExpression... left) {
        List<Mutable<ILogicalExpression>> args = new ArrayList<>();

        for (ILogicalExpression expr : left) {
            args.add(new MutableObject<>(expr));
        }

        IFunctionInfo funcInfo = BuiltinFunctions.getBuiltinFunctionInfo(fid);
        return new ScalarFunctionCallExpression(funcInfo, args);
    }

    private ILogicalExpression createExpression(FunctionIdentifier fid, char... left) {
        List<Mutable<ILogicalExpression>> args = new ArrayList<>();

        for (int i = 0; i < left.length; i++) {
            args.add(new MutableObject<>(getVariableExpression(left[i])));
        }

        IFunctionInfo funcInfo = BuiltinFunctions.getBuiltinFunctionInfo(fid);
        return new ScalarFunctionCallExpression(funcInfo, args);
    }

    private ILogicalExpression getVariableExpression(Character displayName) {
        LogicalVariable variable = varNameToVarMap.computeIfAbsent(displayName,
                k -> new LogicalVariable(varCounter++, displayName.toString()));
        return new VariableReferenceExpression(variable);
    }
}
