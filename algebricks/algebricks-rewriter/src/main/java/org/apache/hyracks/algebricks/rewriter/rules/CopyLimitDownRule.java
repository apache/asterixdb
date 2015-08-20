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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical.StreamLimitPOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

public class CopyLimitDownRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.LIMIT) {
            return false;
        }
        LimitOperator limitOp = (LimitOperator) op;
        if (!limitOp.isTopmostLimitOp()) {
            return false;
        }

        List<LogicalVariable> limitUsedVars = new ArrayList<>();
        VariableUtilities.getUsedVariables(limitOp, limitUsedVars);

        Mutable<ILogicalOperator> safeOpRef = null;
        Mutable<ILogicalOperator> candidateOpRef = limitOp.getInputs().get(0);

        List<LogicalVariable> candidateProducedVars = new ArrayList<>();
        while (true) {
            candidateProducedVars.clear();
            ILogicalOperator candidateOp = candidateOpRef.getValue();
            LogicalOperatorTag candidateOpTag = candidateOp.getOperatorTag();
            if (candidateOp.getInputs().size() > 1 || !candidateOp.isMap()
                    || candidateOpTag == LogicalOperatorTag.SELECT || candidateOpTag == LogicalOperatorTag.LIMIT
                    || !OperatorPropertiesUtil.disjoint(limitUsedVars, candidateProducedVars)) {
                break;
            }

            safeOpRef = candidateOpRef;
            candidateOpRef = safeOpRef.getValue().getInputs().get(0);
        }

        if (safeOpRef != null) {
            ILogicalOperator safeOp = safeOpRef.getValue();
            Mutable<ILogicalOperator> unsafeOpRef = safeOp.getInputs().get(0);
            ILogicalOperator unsafeOp = unsafeOpRef.getValue();
            LimitOperator limitCloneOp = null;
            if (limitOp.getOffset().getValue() == null) {
                limitCloneOp = new LimitOperator(limitOp.getMaxObjects().getValue(), false);
            } else {
                IFunctionInfo finfoAdd = context.getMetadataProvider().lookupFunction(
                        AlgebricksBuiltinFunctions.NUMERIC_ADD);
                List<Mutable<ILogicalExpression>> addArgs = new ArrayList<>();
                addArgs.add(new MutableObject<ILogicalExpression>(limitOp.getMaxObjects().getValue().cloneExpression()));
                addArgs.add(new MutableObject<ILogicalExpression>(limitOp.getOffset().getValue().cloneExpression()));
                ScalarFunctionCallExpression maxPlusOffset = new ScalarFunctionCallExpression(finfoAdd, addArgs);
                limitCloneOp = new LimitOperator(maxPlusOffset, false);
            }
            limitCloneOp.setPhysicalOperator(new StreamLimitPOperator());
            limitCloneOp.getInputs().add(new MutableObject<ILogicalOperator>(unsafeOp));
            limitCloneOp.setExecutionMode(unsafeOp.getExecutionMode());
            limitCloneOp.recomputeSchema();
            unsafeOpRef.setValue(limitCloneOp);
            context.computeAndSetTypeEnvironmentForOperator(limitCloneOp);
            context.addToDontApplySet(this, limitOp);
        }

        return safeOpRef != null;
    }
}
