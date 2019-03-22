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

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.algebra.visitors.ILogicalOperatorVisitor;

public class VariableUtilities {

    /***
     * Adds the used variables in the logical operator to the list of used variables
     *
     * @param op
     *            The target operator
     * @param usedVariables
     *            A list to be filled with variables used in the logical operator op.
     * @throws AlgebricksException
     */
    public static void getUsedVariables(ILogicalOperator op, Collection<LogicalVariable> usedVariables)
            throws AlgebricksException {
        ILogicalOperatorVisitor<Void, Void> visitor = new UsedVariableVisitor(usedVariables);
        op.accept(visitor, null);
    }

    /**
     * Adds the variables produced in the logical operator in the list of produced variables
     *
     * @param op
     *            The target operator
     * @param producedVariables
     *            The variables produced in the logical operator
     * @throws AlgebricksException
     */
    public static void getProducedVariables(ILogicalOperator op, Collection<LogicalVariable> producedVariables)
            throws AlgebricksException {
        ILogicalOperatorVisitor<Void, Void> visitor = new ProducedVariableVisitor(producedVariables);
        op.accept(visitor, null);
    }

    /**
     * Adds the variables that are live after the execution of this operator to the list of schema variables.
     *
     * @param op
     *            The target logical operator
     * @param schemaVariables
     *            The list of live variables. The output of the operator and the propagated outputs of its children
     * @throws AlgebricksException
     */
    public static void getLiveVariables(ILogicalOperator op, Collection<LogicalVariable> schemaVariables)
            throws AlgebricksException {
        ILogicalOperatorVisitor<Void, Void> visitor = new SchemaVariableVisitor(schemaVariables);
        op.accept(visitor, null);
    }

    public static void getSubplanLocalLiveVariables(ILogicalOperator op, Collection<LogicalVariable> liveVariables)
            throws AlgebricksException {
        VariableUtilities.getLiveVariables(op, liveVariables);
        Set<LogicalVariable> locallyProducedVars = new HashSet<>();
        VariableUtilities.getProducedVariablesInDescendantsAndSelf(op, locallyProducedVars);
        liveVariables.retainAll(locallyProducedVars);
    }

    public static void getUsedVariablesInDescendantsAndSelf(ILogicalOperator op, Collection<LogicalVariable> vars)
            throws AlgebricksException {
        // DFS traversal
        VariableUtilities.getUsedVariables(op, vars);
        for (Mutable<ILogicalOperator> c : op.getInputs()) {
            getUsedVariablesInDescendantsAndSelf(c.getValue(), vars);
        }
    }

    public static void getProducedVariablesInDescendantsAndSelf(ILogicalOperator op, Collection<LogicalVariable> vars)
            throws AlgebricksException {
        // DFS traversal
        VariableUtilities.getProducedVariables(op, vars);
        for (Mutable<ILogicalOperator> c : op.getInputs()) {
            getProducedVariablesInDescendantsAndSelf(c.getValue(), vars);
        }
    }

    public static void substituteVariables(ILogicalOperator op, LogicalVariable v1, LogicalVariable v2,
            ITypingContext ctx) throws AlgebricksException {
        substituteVariables(op, v1, v2, true, ctx);
    }

    /**
     * Substitute variable references inside an operator according to a variable map.
     *
     * @param op,
     *            the operator for variable substitution.
     * @param varMap,
     *            a map that maps old variables to new variables. Note that we enforce
     *            the map to be a LinkedHashMap so as to be able to perform recursive
     *            variable substitution within one pass. The order of variable substitution
     *            is the entry insertion order of the LinkedHashMap. Typically, the LinkedHashMap
     *            is populated by recursively, bottom-up traversing of the query plan.
     *            For example, if $1->$2 and $2->$3 are orderly inserted into the map, $1 will be
     *            replaced by $3 in the outcome of this method call.
     *
     * @param ctx,
     *            a typing context that keeps track of types of each variable.
     * @throws AlgebricksException
     */
    public static void substituteVariables(ILogicalOperator op, LinkedHashMap<LogicalVariable, LogicalVariable> varMap,
            ITypingContext ctx) throws AlgebricksException {
        for (Map.Entry<LogicalVariable, LogicalVariable> entry : varMap.entrySet()) {
            VariableUtilities.substituteVariables(op, entry.getKey(), entry.getValue(), ctx);
        }
    }

    public static void substituteVariables(ILogicalOperator op,
            List<Pair<LogicalVariable, LogicalVariable>> oldVarNewVarMapHistory, ITypingContext ctx)
            throws AlgebricksException {
        for (Pair<LogicalVariable, LogicalVariable> entry : oldVarNewVarMapHistory) {
            VariableUtilities.substituteVariables(op, entry.first, entry.second, ctx);
        }
    }

    public static void substituteVariablesInDescendantsAndSelf(ILogicalOperator op, LogicalVariable v1,
            LogicalVariable v2, ITypingContext ctx) throws AlgebricksException {
        for (Mutable<ILogicalOperator> childOp : op.getInputs()) {
            substituteVariablesInDescendantsAndSelf(childOp.getValue(), v1, v2, ctx);
        }
        substituteVariables(op, v1, v2, true, ctx);
    }

    public static void substituteVariablesInDescendantsAndSelf(ILogicalOperator op,
            Map<LogicalVariable, LogicalVariable> varMap, ITypingContext ctx) throws AlgebricksException {
        for (Entry<LogicalVariable, LogicalVariable> entry : varMap.entrySet()) {
            for (Mutable<ILogicalOperator> childOp : op.getInputs()) {
                substituteVariablesInDescendantsAndSelf(childOp.getValue(), entry.getKey(), entry.getValue(), ctx);
            }
            substituteVariables(op, entry.getKey(), entry.getValue(), true, ctx);
        }
    }

    public static void substituteVariables(ILogicalOperator op, LogicalVariable v1, LogicalVariable v2,
            boolean goThroughNts, ITypingContext ctx) throws AlgebricksException {
        ILogicalOperatorVisitor<Void, Pair<LogicalVariable, LogicalVariable>> visitor =
                new SubstituteVariableVisitor(goThroughNts, ctx);
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
