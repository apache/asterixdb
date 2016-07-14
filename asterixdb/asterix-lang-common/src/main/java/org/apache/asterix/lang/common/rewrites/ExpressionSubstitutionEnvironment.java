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
package org.apache.asterix.lang.common.rewrites;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.lang.common.base.Expression;
import org.apache.asterix.lang.common.base.ILangExpression;
import org.apache.asterix.lang.common.expression.VariableExpr;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

/**
 * This class is in charge of substitute expressions by a given mapping while
 * traversing a given AST. The traversal and expression substitution should
 * be based on the correct variable scoping. In order to support scoping at the
 * caller, this class provides methods like mark, reset and pop.
 */
public class ExpressionSubstitutionEnvironment {

    @FunctionalInterface
    public static interface FreeVariableCollector {
        public Collection<VariableExpr> getFreeVariable(ILangExpression expr) throws AsterixException;
    }

    @FunctionalInterface
    public static interface DeepCopier {
        public ILangExpression deepCopy(ILangExpression expr) throws AsterixException;
    }

    private Map<Expression, Expression> exprMap = new HashMap<>();
    private Map<VariableExpr, Expression> freeVarToExprMap = new HashMap<>();

    // We use multiset here because variables can be defined multiple times
    // in the scope stack.
    private Multiset<Expression> disabledExpr = HashMultiset.create();

    // Snapshots of variables that should be disabled for replacement,
    // e.g., if a variable is redefined in a closer scope.
    private Deque<Multiset<Expression>> disabledExprBackup = new ArrayDeque<>();

    public ExpressionSubstitutionEnvironment() {
        // Default constructor.
    }

    public ExpressionSubstitutionEnvironment(Map<Expression, Expression> map, FreeVariableCollector freeVarCollector)
            throws AsterixException {
        addMapping(map, freeVarCollector);
    }

    /**
     * Finds a substitution expression.
     *
     * @param expr
     *            the original expression.
     * @return the new, replaced expression.
     */
    public Expression findSubstitution(Expression expr, DeepCopier deepCopier) throws AsterixException {
        Expression replacementExpr = exprMap.get(expr);
        if (replacementExpr != null && !disabledExpr.contains(replacementExpr)) {
            return (Expression) deepCopier.deepCopy(replacementExpr);
        }
        return expr;
    }

    /**
     * Disable a substitution when a free variable in the expression is re-defined.
     *
     * @param var
     *            a re-defined variable.
     */
    public void disableVariable(VariableExpr var) {
        Expression expr = freeVarToExprMap.get(var);
        if (expr != null) {
            disabledExpr.add(expr);
        }
    }

    /**
     * Re-enable a substitution when a re-defined variable exits its scope.
     *
     * @param var
     *            a re-defined variable.
     */
    public void enableVariable(VariableExpr var) {
        Expression expr = freeVarToExprMap.get(var);
        if (expr != null) {
            disabledExpr.remove(expr);
        }
    }

    /**
     * Tasks a snapshot of the current states.
     *
     * @return the snapshot id that can be reset to in the future.
     */
    public int mark() {
        Multiset<Expression> copyOfDisabledExprs = HashMultiset.create();
        copyOfDisabledExprs.addAll(disabledExpr);
        disabledExprBackup.push(copyOfDisabledExprs);
        return disabledExprBackup.size() - 1;
    }

    /**
     * Resets the internal states to a snapshot.
     *
     * @param depth,
     *            the snapshot id that the caller wants to recover to.
     */
    public void reset(int depth) {
        while (disabledExprBackup.size() > depth) {
            disabledExpr = disabledExprBackup.pop();
        }
    }

    /**
     * Restores to the most-recent snapshot.
     */
    public void pop() {
        if (!disabledExprBackup.isEmpty()) {
            disabledExpr = disabledExprBackup.pop();
        }
    }

    @Override
    public String toString() {
        return exprMap.toString();
    }

    private void addMapping(Map<Expression, Expression> map, FreeVariableCollector freeVarCollector)
            throws AsterixException {
        exprMap.putAll(map);
        // Put free variable to target expression map.
        for (Entry<Expression, Expression> entry : map.entrySet()) {
            Expression targetExpr = entry.getKey();
            for (VariableExpr freeVar : freeVarCollector.getFreeVariable(targetExpr)) {
                freeVarToExprMap.put(freeVar, targetExpr);
            }
        }
    }

}
