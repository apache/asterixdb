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
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LeftOuterUnnestMapOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * In case {@link LeftOuterJoinOperator} gets transformed into {@link LeftOuterUnnestMapOperator}, the plan could
 * end up with multiple SELECT operators (broken up by {@link BreakSelectIntoConjunctsRule}).
 * This rule consolidate those SELECT operators back again.
 * <p>
 * Example:
 * select (and([JOIN_CONDITION], eq($$o.getField("x"), 1)))
 * -- unnest $$o <- dataset(...)
 * <p>
 * After {@link BreakSelectIntoConjunctsRule}
 * select ([JOIN_CONDITION])
 * -- select (eq($$o.getField("x"), 1))
 * -- -- unnest $$o <- dataset(...)
 * <p>
 * Before accessMethod rewrite:
 * left outer join ([JOIN_CONDITION])
 * -- data-scan []<-[$$56, $$c] <- ...
 * -- select (eq($$o.getField("x"), 1))
 * -- -- data-scan []<-[$$57, $$o] <- ...
 * <p>
 * After accessMethod rewrite:
 * select ([JOIN_CONDITION]) retain-untrue (... <- missing)
 * -- select (eq($$o.getField("x"), 1))
 * -- -- left-outer-unnest-map ...
 * -- -- -- ...
 * <p>
 * After this rule:
 * select (and([JOIN_CONDITION], eq($$o.getField("x"), 1))) retain-untrue (... <- missing)
 * -- left-outer-unnest-map ...
 */
public class ConsolidateLeftOuterJoinSelectsRule implements IAlgebraicRewriteRule {
    private final List<Mutable<ILogicalExpression>> conditions = new ArrayList<>();

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator op = opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.SELECT) {
            return false;
        }

        conditions.clear();
        IAlgebricksConstantValue missingValue = null;
        LogicalVariable missingVariable = null;
        ILogicalOperator nextOp = op;
        do {
            SelectOperator selectOp = (SelectOperator) nextOp;
            if (missingValue == null) {
                // Capture the SELECT that contains the retain missing value and variable placeholder
                missingValue = selectOp.getRetainMissingAsValue();
                missingVariable = selectOp.getMissingPlaceholderVariable();
            }
            conditions.add(new MutableObject<>(selectOp.getCondition().getValue()));
            nextOp = nextOp.getInputs().get(0).getValue();
        } while (nextOp.getOperatorTag() == LogicalOperatorTag.SELECT);

        if (conditions.size() < 2 || missingValue == null) {
            return false;
        }

        SelectOperator newSelect = new SelectOperator(createAndCondition(context), missingValue, missingVariable);
        newSelect.getInputs().add(new MutableObject<>(nextOp));
        opRef.setValue(newSelect);
        context.computeAndSetTypeEnvironmentForOperator(newSelect);
        return true;
    }

    private Mutable<ILogicalExpression> createAndCondition(IOptimizationContext context) {
        IFunctionInfo fInfo = context.getMetadataProvider().lookupFunction(AlgebricksBuiltinFunctions.AND);
        return new MutableObject<>(new ScalarFunctionCallExpression(fInfo, new ArrayList<>(conditions)));
    }
}
