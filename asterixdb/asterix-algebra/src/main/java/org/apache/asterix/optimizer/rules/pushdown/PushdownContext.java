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

import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.optimizer.rules.pushdown.descriptor.DefineDescriptor;
import org.apache.asterix.optimizer.rules.pushdown.descriptor.ScanDefineDescriptor;
import org.apache.asterix.optimizer.rules.pushdown.descriptor.UseDescriptor;
import org.apache.asterix.optimizer.rules.pushdown.visitor.FilterExpressionInlineVisitor;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
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
    private final FilterExpressionInlineVisitor inlineVisitor;
    private final Map<Dataset, List<ScanDefineDescriptor>> datasetToScans;
    private ILogicalOperator currentSubplan;

    public PushdownContext(IOptimizationContext context) {
        registeredScans = new ArrayList<>();
        this.definedVariable = new HashMap<>();
        this.defineChain = new HashMap<>();
        this.useChain = new HashMap<>();
        scopes = new ArrayList<>();
        inlineVisitor = new FilterExpressionInlineVisitor(this, context);
        datasetToScans = new HashMap<>();
    }

    public void enterScope(ILogicalOperator operator) {
        LogicalOperatorTag opTag = operator.getOperatorTag();
        if (SCOPE_OPERATORS.contains(opTag)) {
            scopes.add(operator);
        } else if (opTag == LogicalOperatorTag.AGGREGATE && currentSubplan == null) {
            // Advance scope for aggregate if the aggregate is not in a subplan
            scopes.add(operator);
        }
    }

    public ILogicalOperator enterSubplan(ILogicalOperator subplanOp) {
        ILogicalOperator previous = currentSubplan;
        currentSubplan = subplanOp;
        return previous;
    }

    public void exitSubplan(ILogicalOperator previousSubplan) {
        currentSubplan = previousSubplan;
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
        List<ScanDefineDescriptor> datasetScans = datasetToScans.computeIfAbsent(dataset, k -> new ArrayList<>());
        datasetScans.add(scanDefDesc);
    }

    public Map<Dataset, List<ScanDefineDescriptor>> getDatasetToScanDefinitionDescriptors() {
        return datasetToScans;
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
                new DefineDescriptor(scope, currentSubplan, variable, operator, expression, expressionIndex);
        definedVariable.put(expression, defineDescriptor);
        defineChain.put(variable, defineDescriptor);
        useChain.put(variable, new ArrayList<>());
    }

    public void use(ILogicalOperator operator, ILogicalExpression expression, int expressionIndex,
            LogicalVariable producedVariable) {
        int scope = scopes.size();
        UseDescriptor useDescriptor =
                new UseDescriptor(scope, currentSubplan, operator, expression, expressionIndex, producedVariable);
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

    public FilterExpressionInlineVisitor getInlineVisitor() {
        return inlineVisitor;
    }

    private static Set<LogicalOperatorTag> getScopeOperators() {
        return EnumSet.of(LogicalOperatorTag.INNERJOIN, LogicalOperatorTag.LEFTOUTERJOIN, LogicalOperatorTag.GROUP,
                LogicalOperatorTag.WINDOW, LogicalOperatorTag.RUNNINGAGGREGATE, LogicalOperatorTag.UNIONALL,
                LogicalOperatorTag.INTERSECT);
    }

}
