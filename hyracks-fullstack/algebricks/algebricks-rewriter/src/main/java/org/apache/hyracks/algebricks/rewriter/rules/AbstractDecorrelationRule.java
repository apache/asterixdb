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

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.FunctionalDependency;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.algebricks.rewriter.util.PhysicalOptimizationsUtil;
import org.apache.hyracks.api.exceptions.SourceLocation;

public abstract class AbstractDecorrelationRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    protected boolean descOrSelfIsScanOrJoin(ILogicalOperator op2) {
        LogicalOperatorTag t = op2.getOperatorTag();
        if (isScanOrJoin(t)) {
            return true;
        }
        if (op2.getInputs().size() != 1) {
            return false;
        }
        ILogicalOperator alo = op2.getInputs().get(0).getValue();
        if (descOrSelfIsScanOrJoin(alo)) {
            return true;
        }
        return false;
    }

    protected boolean isScanOrJoin(LogicalOperatorTag t) {
        if (t == LogicalOperatorTag.DATASOURCESCAN || t == LogicalOperatorTag.INNERJOIN
                || t == LogicalOperatorTag.UNNEST || t == LogicalOperatorTag.UNNEST_MAP
                || t == LogicalOperatorTag.LEFTOUTERJOIN) {
            return true;
        }
        return false;
    }

    protected Set<LogicalVariable> computeGbyVarsUsingPksOnly(Set<LogicalVariable> varSet, AbstractLogicalOperator op,
            IOptimizationContext context) throws AlgebricksException {
        PhysicalOptimizationsUtil.computeFDsAndEquivalenceClasses(op, context);
        List<FunctionalDependency> fdList = context.getFDList(op);
        if (fdList == null) {
            return null;
        }
        // check if any of the FDs is a key
        for (FunctionalDependency fd : fdList) {
            if (fd.getTail().containsAll(varSet)) {
                return new HashSet<LogicalVariable>(fd.getHead());
            }
        }
        return null;
    }

    protected void buildVarExprList(Collection<LogicalVariable> vars, IOptimizationContext context, GroupByOperator g,
            List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> outVeList) throws AlgebricksException {
        SourceLocation sourceLoc = g.getSourceLocation();
        for (LogicalVariable ov : vars) {
            LogicalVariable newVar = context.newVar();
            VariableReferenceExpression varExpr = new VariableReferenceExpression(newVar);
            varExpr.setSourceLocation(sourceLoc);
            outVeList.add(new Pair<LogicalVariable, Mutable<ILogicalExpression>>(ov,
                    new MutableObject<ILogicalExpression>(varExpr)));
            for (ILogicalPlan p : g.getNestedPlans()) {
                for (Mutable<ILogicalOperator> r : p.getRoots()) {
                    OperatorManipulationUtil.substituteVarRec((AbstractLogicalOperator) r.getValue(), ov, newVar, true,
                            context);
                }
            }
        }
    }

}
