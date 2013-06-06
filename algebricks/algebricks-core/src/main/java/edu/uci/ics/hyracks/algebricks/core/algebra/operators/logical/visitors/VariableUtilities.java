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
package edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.typing.ITypingContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class VariableUtilities {

    public static void getUsedVariables(ILogicalOperator op, Collection<LogicalVariable> usedVariables)
            throws AlgebricksException {
        ILogicalOperatorVisitor<Void, Void> visitor = new UsedVariableVisitor(usedVariables);
        op.accept(visitor, null);
    }

    public static void getProducedVariables(ILogicalOperator op, Collection<LogicalVariable> producedVariables)
            throws AlgebricksException {
        ILogicalOperatorVisitor<Void, Void> visitor = new ProducedVariableVisitor(producedVariables);
        op.accept(visitor, null);
    }

    public static void getLiveVariables(ILogicalOperator op, Collection<LogicalVariable> schemaVariables)
            throws AlgebricksException {
        ILogicalOperatorVisitor<Void, Void> visitor = new SchemaVariableVisitor(schemaVariables);
        op.accept(visitor, null);
    }

    public static void getUsedVariablesInDescendantsAndSelf(ILogicalOperator op, Collection<LogicalVariable> vars)
            throws AlgebricksException {
        // DFS traversal
        VariableUtilities.getUsedVariables(op, vars);
        for (Mutable<ILogicalOperator> c : op.getInputs()) {
            getUsedVariablesInDescendantsAndSelf(c.getValue(), vars);
        }
    }

    public static void substituteVariables(ILogicalOperator op, LogicalVariable v1, LogicalVariable v2,
            ITypingContext ctx) throws AlgebricksException {
        substituteVariables(op, v1, v2, true, ctx);
    }
    
    public static void substituteVariablesInDescendantsAndSelf(ILogicalOperator op, LogicalVariable v1,
            LogicalVariable v2, ITypingContext ctx) throws AlgebricksException {
        for (Mutable<ILogicalOperator> childOp : op.getInputs()) {
            substituteVariablesInDescendantsAndSelf(childOp.getValue(), v1, v2, ctx);
        }
        substituteVariables(op, v1, v2, true, ctx);
    }
    
    public static void substituteVariables(ILogicalOperator op, LogicalVariable v1, LogicalVariable v2,
            boolean goThroughNts, ITypingContext ctx) throws AlgebricksException {
        ILogicalOperatorVisitor<Void, Pair<LogicalVariable, LogicalVariable>> visitor = new SubstituteVariableVisitor(
                goThroughNts, ctx);
        op.accept(visitor, new Pair<LogicalVariable, LogicalVariable>(v1, v2));
    }
    
    public static <T> boolean varListEqualUnordered(List<T> var, List<T> varArg) {
        Set<T> varSet = new HashSet<T>();
        Set<T> varArgSet = new HashSet<T>();
        varSet.addAll(var);
        varArgSet.addAll(varArg);
        return varSet.equals(varArgSet);
    }

}
