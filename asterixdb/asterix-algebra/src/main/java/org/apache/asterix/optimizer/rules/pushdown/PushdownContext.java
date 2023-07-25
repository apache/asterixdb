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
package org.apache.asterix.optimizer.rules.pushdown;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.optimizer.rules.pushdown.descriptor.DefineDescriptor;
import org.apache.asterix.optimizer.rules.pushdown.descriptor.ScanDefineDescriptor;
import org.apache.asterix.optimizer.rules.pushdown.descriptor.UseDescriptor;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractScanOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PushdownContext {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final Set<LogicalOperatorTag> SCOPE_OPERATORS = getScopeOperators();
    private final List<ScanDefineDescriptor> registeredScans;
    // For debugging purposes only
    private final Map<ILogicalExpression, DefineDescriptor> definedVariable;
    private final Map<LogicalVariable, DefineDescriptor> defineChain;
    private final Map<LogicalVariable, List<UseDescriptor>> useChain;
    private final List<ILogicalOperator> scopes;
    private final Map<ILogicalOperator, ILogicalExpression> inlinedCache;

    public PushdownContext() {
        registeredScans = new ArrayList<>();
        this.definedVariable = new HashMap<>();
        this.defineChain = new HashMap<>();
        this.useChain = new HashMap<>();
        scopes = new ArrayList<>();
        inlinedCache = new HashMap<>();
    }

    public void enterScope(ILogicalOperator operator) {
        if (SCOPE_OPERATORS.contains(operator.getOperatorTag())) {
            scopes.add(operator);
        }
    }

    public void registerScan(Dataset dataset, List<LogicalVariable> pkList, LogicalVariable recordVariable,
            LogicalVariable metaVariable, AbstractScanOperator scanOperator) {
        ScanDefineDescriptor scanDefDesc =
                new ScanDefineDescriptor(scopes.size(), dataset, pkList, recordVariable, metaVariable, scanOperator);
        defineChain.put(recordVariable, scanDefDesc);
        useChain.put(recordVariable, new ArrayList<>());
        if (metaVariable != null) {
            defineChain.put(metaVariable, scanDefDesc);
            useChain.put(metaVariable, new ArrayList<>());
        }
        for (LogicalVariable pkVar : pkList) {
            defineChain.put(pkVar, scanDefDesc);
            useChain.put(pkVar, new ArrayList<>());
        }
        registeredScans.add(scanDefDesc);
    }

    public void define(LogicalVariable variable, ILogicalOperator operator, ILogicalExpression expression,
            int expressionIndex) {
        if (defineChain.containsKey(variable)) {
            LOGGER.warn("Variable {}  declared twice", variable);
            return;
        } else if (definedVariable.containsKey(expression)) {
            DefineDescriptor defineDescriptor = definedVariable.get(expression);
            /*
             * Currently, we know that scan-collection of some constant array can appear multiple times as REPLICATE
             * wasn't fired yet to remove common operators. However, this log is to track any issue could occur due to
             * having redundant expressions declared in different operators.
             */
            LOGGER.debug("Expression {} is redundant. It was seen at {}", expression, defineDescriptor.toString());
        }

        int scope = scopes.size();
        DefineDescriptor defineDescriptor =
                new DefineDescriptor(scope, variable, operator, expression, expressionIndex);
        definedVariable.put(expression, defineDescriptor);
        defineChain.put(variable, defineDescriptor);
        useChain.put(variable, new ArrayList<>());
    }

    public void use(ILogicalOperator operator, ILogicalExpression expression, int expressionIndex,
            LogicalVariable producedVariable) {
        int scope = scopes.size();
        UseDescriptor useDescriptor = new UseDescriptor(scope, operator, expression, expressionIndex, producedVariable);
        Set<LogicalVariable> usedVariables = useDescriptor.getUsedVariables();
        expression.getUsedVariables(usedVariables);
        for (LogicalVariable variable : usedVariables) {
            DefineDescriptor defineDescriptor = defineChain.get(variable);
            if (defineDescriptor == null) {
                // Log to track any definition that we may have missed
                LOGGER.warn("Variable {} is not defined", variable);
                return;
            }

            List<UseDescriptor> uses = useChain.get(variable);
            uses.add(useDescriptor);
        }
    }

    public DefineDescriptor getDefineDescriptor(LogicalVariable variable) {
        return defineChain.get(variable);
    }

    public DefineDescriptor getDefineDescriptor(UseDescriptor useDescriptor) {
        LogicalVariable producedVariable = useDescriptor.getProducedVariable();
        if (producedVariable == null) {
            return null;
        }
        return getDefineDescriptor(producedVariable);
    }

    public List<UseDescriptor> getUseDescriptors(DefineDescriptor defineDescriptor) {
        return useChain.get(defineDescriptor.getVariable());
    }

    public List<ScanDefineDescriptor> getRegisteredScans() {
        return registeredScans;
    }

    public ILogicalExpression cloneAndInlineExpression(UseDescriptor useDescriptor) throws CompilationException {
        ILogicalOperator op = useDescriptor.getOperator();
        ILogicalExpression inlinedExpr = inlinedCache.get(op);
        if (inlinedExpr == null) {
            inlinedExpr = cloneAndInline(useDescriptor.getExpression());
            inlinedCache.put(op, inlinedExpr);
        }

        // Clone the cached expression as a processor may change it
        return inlinedExpr.cloneExpression();
    }

    private ILogicalExpression cloneAndInline(ILogicalExpression expression) throws CompilationException {
        switch (expression.getExpressionTag()) {
            case CONSTANT:
                return expression;
            case FUNCTION_CALL:
                return cloneAndInlineFunction(expression);
            case VARIABLE:
                LogicalVariable variable = ((VariableReferenceExpression) expression).getVariableReference();
                DefineDescriptor defineDescriptor = defineChain.get(variable);
                if (defineDescriptor.isScanDefinition()) {
                    // Reached the recordVariable
                    return expression;
                }
                return cloneAndInline(defineDescriptor.getExpression());
            default:
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, expression.getSourceLocation());
        }
    }

    private ILogicalExpression cloneAndInlineFunction(ILogicalExpression expression) throws CompilationException {
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expression.cloneExpression();
        for (Mutable<ILogicalExpression> arg : funcExpr.getArguments()) {
            arg.setValue(cloneAndInline(arg.getValue()));
        }
        return funcExpr;
    }

    private static Set<LogicalOperatorTag> getScopeOperators() {
        return EnumSet.of(LogicalOperatorTag.INNERJOIN, LogicalOperatorTag.LEFTOUTERJOIN, LogicalOperatorTag.GROUP,
                LogicalOperatorTag.AGGREGATE, LogicalOperatorTag.WINDOW);
    }

}
