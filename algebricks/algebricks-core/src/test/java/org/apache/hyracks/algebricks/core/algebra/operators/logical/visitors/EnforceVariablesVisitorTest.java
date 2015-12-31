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
package org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.junit.Test;

import junit.framework.Assert;

public class EnforceVariablesVisitorTest {

    /**
     * Tests the processing of project operator in RecoverVariablesVisitor.
     *
     * @throws Exception
     */
    @Test
    public void testProject() throws Exception {
        // Constructs the input operator.
        LogicalVariable var = new LogicalVariable(1);
        List<LogicalVariable> inputVarList = new ArrayList<>();
        inputVarList.add(var);
        ProjectOperator projectOp = new ProjectOperator(inputVarList);

        // Constructs the visitor.
        IOptimizationContext mockedContext = mock(IOptimizationContext.class);
        EnforceVariablesVisitor visitor = new EnforceVariablesVisitor(mockedContext);

        // Calls the visitor.
        LogicalVariable varToEnforce = new LogicalVariable(2);
        ProjectOperator op = (ProjectOperator) projectOp.accept(visitor,
                Arrays.asList(new LogicalVariable[] { varToEnforce }));

        // Checks the result.
        List<LogicalVariable> expectedVars = Arrays.asList(new LogicalVariable[] { var, varToEnforce });
        Assert.assertEquals(expectedVars, op.getVariables());
        Assert.assertTrue(visitor.getInputVariableToOutputVariableMap().isEmpty());
    }

    /**
     * Tests the processing of group-by operator in RecoverVariablesVisitor.
     *
     * @throws Exception
     */
    @Test
    public void testGroupby() throws Exception {
        // Constructs the group-by operator.
        LogicalVariable keyVar = new LogicalVariable(2);
        LogicalVariable keyExprVar = new LogicalVariable(1);
        GroupByOperator gbyOp = new GroupByOperator();
        gbyOp.getGroupByList().add(new Pair<LogicalVariable, Mutable<ILogicalExpression>>(keyVar,
                new MutableObject<ILogicalExpression>(new VariableReferenceExpression(keyExprVar))));

        // Constructs the visitor.
        IOptimizationContext mockedContext = mock(IOptimizationContext.class);
        EnforceVariablesVisitor visitor = new EnforceVariablesVisitor(mockedContext);

        // Calls the visitor.
        LogicalVariable varToEnforce = new LogicalVariable(3);
        Set<LogicalVariable> varsToEnforce = new HashSet<>();
        varsToEnforce.add(keyExprVar);
        varsToEnforce.add(varToEnforce);
        GroupByOperator op = (GroupByOperator) gbyOp.accept(visitor, varsToEnforce);

        // Checks the result.
        Map<LogicalVariable, LogicalVariable> expectedVarMap = new HashMap<>();
        expectedVarMap.put(keyExprVar, keyVar);
        Assert.assertEquals(expectedVarMap, visitor.getInputVariableToOutputVariableMap());
        VariableReferenceExpression decorVarExpr = (VariableReferenceExpression) op.getDecorList().get(0).second
                .getValue();
        Assert.assertEquals(decorVarExpr.getVariableReference(), varToEnforce);
    }

    /**
     * Tests the processing of aggregate operator in RecoverVariablesVisitor.
     *
     * @throws Exception
     */
    @Test
    public void testAggregate() throws Exception {
        // Constructs the group-by operator.
        List<LogicalVariable> aggVars = new ArrayList<>();
        List<Mutable<ILogicalExpression>> aggExprRefs = new ArrayList<>();
        AggregateOperator aggOp = new AggregateOperator(aggVars, aggExprRefs);

        // Constructs the visitor.
        LogicalVariable var = new LogicalVariable(3);
        IOptimizationContext mockedContext = mock(IOptimizationContext.class);
        when(mockedContext.newVar()).thenReturn(var);
        EnforceVariablesVisitor visitor = new EnforceVariablesVisitor(mockedContext);

        // Calls the visitor.
        LogicalVariable varToEnforce = new LogicalVariable(2);
        Set<LogicalVariable> varsToEnforce = new HashSet<>();
        varsToEnforce.add(varToEnforce);
        GroupByOperator op = (GroupByOperator) aggOp.accept(visitor, varsToEnforce);

        // Checks the result.
        Map<LogicalVariable, LogicalVariable> expectedVarMap = new HashMap<>();
        expectedVarMap.put(varToEnforce, var);
        Assert.assertEquals(expectedVarMap, visitor.getInputVariableToOutputVariableMap());
        VariableReferenceExpression keyExpr = (VariableReferenceExpression) op.getGroupByList().get(0).second
                .getValue();
        Assert.assertEquals(keyExpr.getVariableReference(), varToEnforce);
        LogicalVariable expectedGbyVar = op.getGroupByList().get(0).first;
        Assert.assertEquals(expectedGbyVar, var);
    }

    /**
     * Tests the processing of two serial group-by operators in RecoverVariablesVisitor.
     *
     * @throws Exception
     */
    @Test
    public void testTwoGroupbys() throws Exception {
        // Constructs the group-by operators.
        LogicalVariable keyVar = new LogicalVariable(1);
        LogicalVariable keyExprVar = new LogicalVariable(2);
        GroupByOperator gbyOp = new GroupByOperator();
        gbyOp.getGroupByList().add(new Pair<LogicalVariable, Mutable<ILogicalExpression>>(keyVar,
                new MutableObject<ILogicalExpression>(new VariableReferenceExpression(keyExprVar))));
        LogicalVariable keyVar2 = new LogicalVariable(2);
        LogicalVariable keyExprVar2 = new LogicalVariable(3);
        GroupByOperator gbyOp2 = new GroupByOperator();
        gbyOp.getGroupByList().add(new Pair<LogicalVariable, Mutable<ILogicalExpression>>(keyVar2,
                new MutableObject<ILogicalExpression>(new VariableReferenceExpression(keyExprVar2))));
        gbyOp.getInputs().add(new MutableObject<ILogicalOperator>(gbyOp2));

        // Constructs the visitor.
        IOptimizationContext mockedContext = mock(IOptimizationContext.class);
        EnforceVariablesVisitor visitor = new EnforceVariablesVisitor(mockedContext);

        // Calls the visitor.
        LogicalVariable varToEnforce = new LogicalVariable(4);
        Set<LogicalVariable> varsToEnforce = new HashSet<>();
        varsToEnforce.add(keyExprVar2);
        varsToEnforce.add(varToEnforce);
        GroupByOperator op = (GroupByOperator) gbyOp.accept(visitor, varsToEnforce);

        // Checks the result.
        Map<LogicalVariable, LogicalVariable> expectedVarMap = new HashMap<>();
        expectedVarMap.put(keyExprVar2, keyVar);
        Assert.assertEquals(expectedVarMap, visitor.getInputVariableToOutputVariableMap());
        VariableReferenceExpression decorVarExpr = (VariableReferenceExpression) op.getDecorList().get(0).second
                .getValue();
        Assert.assertEquals(decorVarExpr.getVariableReference(), varToEnforce);
        GroupByOperator op2 = (GroupByOperator) op.getInputs().get(0).getValue();
        VariableReferenceExpression decorVarExpr2 = (VariableReferenceExpression) op2.getDecorList().get(0).second
                .getValue();
        Assert.assertEquals(decorVarExpr2.getVariableReference(), varToEnforce);
    }

}
