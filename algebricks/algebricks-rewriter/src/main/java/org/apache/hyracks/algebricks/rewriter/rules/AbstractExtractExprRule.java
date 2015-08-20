/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.algebricks.rewriter.rules;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public abstract class AbstractExtractExprRule implements IAlgebraicRewriteRule {

    protected LogicalVariable extractExprIntoAssignOpRef(ILogicalExpression gExpr, Mutable<ILogicalOperator> opRef2,
            IOptimizationContext context) throws AlgebricksException {
        LogicalVariable v = context.newVar();
        AssignOperator a = new AssignOperator(v, new MutableObject<ILogicalExpression>(gExpr));
        a.getInputs().add(new MutableObject<ILogicalOperator>(opRef2.getValue()));
        opRef2.setValue(a);
        if (gExpr.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            context.addNotToBeInlinedVar(v);
        }
        context.computeAndSetTypeEnvironmentForOperator(a);
        return v;
    }

}
