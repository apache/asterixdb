/*
 * Copyright 2009-2010 by The Regents of the University of California
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

import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;

/**
 * Where assign operators only assign a new variable ID for a reference expression,
 * all references are updated to the first variable ID.
 */
public class InlineVariablePolicy implements InlineVariablesRule.IInlineVariablePolicy {

    @Override
    public boolean isCandidateForInlining(ILogicalExpression expr) {
        if (expr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            return true;
        }
        return false;
    }

    @Override
    public List<Mutable<ILogicalOperator>> descendIntoNextOperator(AbstractLogicalOperator op) {
        List<Mutable<ILogicalOperator>> descendOp = new ArrayList<Mutable<ILogicalOperator>>();
        // Descend into children removing projects on the way.
        for (Mutable<ILogicalOperator> inputOpRef : op.getInputs()) {
            descendOp.add(inputOpRef);
        }
        return descendOp;
    }

    @Override
    public boolean isCanidateInlineTarget(AbstractLogicalOperator op) {
        // Only inline variables in operators that can deal with arbitrary expressions.
        if (!op.requiresVariableReferenceExpressions()) {
            return true;
        }
        return false;
    }

}
