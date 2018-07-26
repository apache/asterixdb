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
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.LimitOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.StreamLimitPOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class CopyLimitDownRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
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
            ILogicalOperator candidateOp = candidateOpRef.getValue();
            LogicalOperatorTag candidateOpTag = candidateOp.getOperatorTag();
            if (candidateOp.getInputs().size() > 1 || !candidateOp.isMap()
                    || candidateOpTag == LogicalOperatorTag.SELECT || candidateOpTag == LogicalOperatorTag.LIMIT
                    || candidateOpTag == LogicalOperatorTag.UNNEST_MAP) {
                break;
            }

            candidateProducedVars.clear();
            VariableUtilities.getProducedVariables(candidateOp, candidateProducedVars);
            if (!OperatorPropertiesUtil.disjoint(limitUsedVars, candidateProducedVars)) {
                break;
            }

            safeOpRef = candidateOpRef;
            candidateOpRef = safeOpRef.getValue().getInputs().get(0);
        }

        if (safeOpRef != null) {
            ILogicalOperator safeOp = safeOpRef.getValue();
            Mutable<ILogicalOperator> unsafeOpRef = safeOp.getInputs().get(0);
            ILogicalOperator unsafeOp = unsafeOpRef.getValue();
            SourceLocation sourceLoc = limitOp.getSourceLocation();
            LimitOperator limitCloneOp = null;
            if (limitOp.getOffset().getValue() == null) {
                limitCloneOp = new LimitOperator(limitOp.getMaxObjects().getValue(), false);
                limitCloneOp.setSourceLocation(sourceLoc);
            } else {
                // Need to add an offset to the given limit value
                // since the original topmost limit will use the offset value.
                // We can't apply the offset multiple times.
                IFunctionInfo finfoAdd =
                        context.getMetadataProvider().lookupFunction(AlgebricksBuiltinFunctions.NUMERIC_ADD);
                List<Mutable<ILogicalExpression>> addArgs = new ArrayList<>();
                addArgs.add(
                        new MutableObject<ILogicalExpression>(limitOp.getMaxObjects().getValue().cloneExpression()));
                addArgs.add(new MutableObject<ILogicalExpression>(limitOp.getOffset().getValue().cloneExpression()));
                ScalarFunctionCallExpression maxPlusOffset = new ScalarFunctionCallExpression(finfoAdd, addArgs);
                maxPlusOffset.setSourceLocation(sourceLoc);
                limitCloneOp = new LimitOperator(maxPlusOffset, false);
                limitCloneOp.setSourceLocation(sourceLoc);
            }
            limitCloneOp.setPhysicalOperator(new StreamLimitPOperator());
            limitCloneOp.getInputs().add(new MutableObject<ILogicalOperator>(unsafeOp));
            limitCloneOp.setExecutionMode(unsafeOp.getExecutionMode());
            OperatorPropertiesUtil.computeSchemaRecIfNull((AbstractLogicalOperator) unsafeOp);
            limitCloneOp.recomputeSchema();
            unsafeOpRef.setValue(limitCloneOp);
            context.computeAndSetTypeEnvironmentForOperator(limitCloneOp);
            context.addToDontApplySet(this, limitOp);
        }

        return safeOpRef != null;
    }
}
