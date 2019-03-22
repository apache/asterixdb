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

package org.apache.hyracks.algebricks.rewriter.rules;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * @author kereno, ecarm002, ildar.absalyamov
 *         Pushes down unnest through both branches of the union operator
 *         Before rule:
 *         ============
 *         unnest
 *         union (left_branch, right_branch, res)
 *         left_branch
 *         right_branch
 *         After rule:
 *         ============
 *         union (left_branch, right_branch, res)
 *         unnest
 *         left_branch
 *         unnest
 *         right_branch
 */
public class PushUnnestDownThroughUnionRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {

        AbstractLogicalOperator unnest = (AbstractLogicalOperator) opRef.getValue();
        if (unnest.getOperatorTag() != LogicalOperatorTag.UNNEST) {
            return false;
        }
        UnnestOperator unnestOpRef = (UnnestOperator) opRef.getValue();
        Mutable<ILogicalOperator> unionOp = unnest.getInputs().get(0);

        AbstractLogicalOperator unionAbstractOp = (AbstractLogicalOperator) unionOp.getValue();
        if (unionAbstractOp.getOperatorTag() != LogicalOperatorTag.UNIONALL) {
            return false;
        }

        LogicalVariable unnestVar1 = context.newVar();
        UnnestOperator unnest1 = new UnnestOperator(unnestVar1,
                new MutableObject<ILogicalExpression>(unnestOpRef.getExpressionRef().getValue().cloneExpression()));
        LogicalVariable unnestVar2 = context.newVar();
        UnnestOperator unnest2 = new UnnestOperator(unnestVar2,
                new MutableObject<ILogicalExpression>(unnestOpRef.getExpressionRef().getValue().cloneExpression()));

        //Getting the two topmost branched and adding them as an input to the unnests:
        Mutable<ILogicalOperator> branch1 = unionAbstractOp.getInputs().get(0);
        ILogicalOperator agg1 = branch1.getValue();
        List<LogicalVariable> agg1_var = new ArrayList<LogicalVariable>();
        VariableUtilities.getLiveVariables(agg1, agg1_var);
        Mutable<ILogicalOperator> branch2 = unionAbstractOp.getInputs().get(1);
        ILogicalOperator agg2 = branch2.getValue();
        List<LogicalVariable> agg2_var = new ArrayList<LogicalVariable>();
        VariableUtilities.getLiveVariables(agg2, agg2_var);

        //Modifying the unnest so it has the right variable
        List<LogicalVariable> var_unnest_1 = new ArrayList<LogicalVariable>();
        unnest1.getExpressionRef().getValue().getUsedVariables(var_unnest_1);
        unnest1.getExpressionRef().getValue().substituteVar(var_unnest_1.get(0), agg1_var.get(0));

        List<LogicalVariable> var_unnest2 = new ArrayList<LogicalVariable>();
        unnest2.getExpressionRef().getValue().getUsedVariables(var_unnest2);
        unnest2.getExpressionRef().getValue().substituteVar(var_unnest2.get(0), agg2_var.get(0));

        unnest1.getInputs().add(branch1);
        unnest2.getInputs().add(branch2);
        context.computeAndSetTypeEnvironmentForOperator(unnest1);
        context.computeAndSetTypeEnvironmentForOperator(unnest2);

        //creating a new union operator with the updated logical variables
        List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> varMap =
                new ArrayList<Triple<LogicalVariable, LogicalVariable, LogicalVariable>>(1);
        Triple<LogicalVariable, LogicalVariable, LogicalVariable> union_triple_vars =
                new Triple<LogicalVariable, LogicalVariable, LogicalVariable>(unnestVar1, unnestVar2,
                        unnestOpRef.getVariables().get(0));
        varMap.add(union_triple_vars);
        UnionAllOperator unionOpFinal = new UnionAllOperator(varMap);

        unionOpFinal.getInputs().add(new MutableObject<ILogicalOperator>(unnest1));
        unionOpFinal.getInputs().add(new MutableObject<ILogicalOperator>(unnest2));

        context.computeAndSetTypeEnvironmentForOperator(unionOpFinal);

        opRef.setValue(unionOpFinal);
        return true;

    }
}
