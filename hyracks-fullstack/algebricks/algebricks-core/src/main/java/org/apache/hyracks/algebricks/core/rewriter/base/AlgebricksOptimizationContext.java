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
package org.apache.hyracks.algebricks.core.rewriter.base;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.EquivalenceClass;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.IConflictingTypeResolver;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionEvalSizeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionTypeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.IMergeAggregationExpressionFactory;
import org.apache.hyracks.algebricks.core.algebra.expressions.IMissableTypeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableEvalSizeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.IPlanPrettyPrinter;
import org.apache.hyracks.algebricks.core.algebra.properties.DefaultNodeGroupDomain;
import org.apache.hyracks.algebricks.core.algebra.properties.FunctionalDependency;
import org.apache.hyracks.algebricks.core.algebra.properties.ILogicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.api.exceptions.IWarningCollector;

/**
 * The Algebricks default implementation for IOptimizationContext.
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class AlgebricksOptimizationContext implements IOptimizationContext {

    private int varCounter;
    private final IExpressionEvalSizeComputer expressionEvalSizeComputer;
    private final IMergeAggregationExpressionFactory mergeAggregationExpressionFactory;
    private final PhysicalOptimizationConfig physicalOptimizationConfig;
    private final IVariableEvalSizeEnvironment varEvalSizeEnv = new IVariableEvalSizeEnvironment() {

        Map<LogicalVariable, Integer> varSizeMap = new HashMap<>();

        @Override
        public void setVariableEvalSize(LogicalVariable var, int size) {
            varSizeMap.put(var, size);
        }

        @Override
        public int getVariableEvalSize(LogicalVariable var) {
            return varSizeMap.get(var);
        }
    };

    private Map<ILogicalOperator, IVariableTypeEnvironment> typeEnvMap = new HashMap<>();

    private Map<ILogicalOperator, HashSet<ILogicalOperator>> alreadyCompared = new HashMap<>();
    private Map<IAlgebraicRewriteRule, HashSet<ILogicalOperator>> dontApply = new HashMap<>();
    private Map<LogicalVariable, FunctionalDependency> varToPrimaryKey = new HashMap<>();

    private IMetadataProvider metadataProvider;
    private HashSet<LogicalVariable> notToBeInlinedVars = new HashSet<>();

    protected final Map<ILogicalOperator, List<FunctionalDependency>> fdGlobalMap = new HashMap<>();
    protected final Map<ILogicalOperator, Map<LogicalVariable, EquivalenceClass>> eqClassGlobalMap = new HashMap<>();

    protected final Map<ILogicalOperator, ILogicalPropertiesVector> logicalProps = new HashMap<>();
    private final IExpressionTypeComputer expressionTypeComputer;
    private final IMissableTypeComputer nullableTypeComputer;
    private final INodeDomain defaultNodeDomain;
    private final IPlanPrettyPrinter prettyPrinter;
    private final IConflictingTypeResolver conflictingTypeResovler;
    private final IWarningCollector warningCollector;

    public AlgebricksOptimizationContext(int varCounter, IExpressionEvalSizeComputer expressionEvalSizeComputer,
            IMergeAggregationExpressionFactory mergeAggregationExpressionFactory,
            IExpressionTypeComputer expressionTypeComputer, IMissableTypeComputer nullableTypeComputer,
            IConflictingTypeResolver conflictingTypeResovler, PhysicalOptimizationConfig physicalOptimizationConfig,
            AlgebricksPartitionConstraint clusterLocations, IPlanPrettyPrinter prettyPrinter,
            IWarningCollector warningCollector) {
        this.varCounter = varCounter;
        this.expressionEvalSizeComputer = expressionEvalSizeComputer;
        this.mergeAggregationExpressionFactory = mergeAggregationExpressionFactory;
        this.expressionTypeComputer = expressionTypeComputer;
        this.nullableTypeComputer = nullableTypeComputer;
        this.physicalOptimizationConfig = physicalOptimizationConfig;
        this.defaultNodeDomain = new DefaultNodeGroupDomain(clusterLocations);
        this.prettyPrinter = prettyPrinter;
        this.conflictingTypeResovler = conflictingTypeResovler;
        this.warningCollector = warningCollector;
    }

    @Override
    public int getVarCounter() {
        return varCounter;
    }

    @Override
    public void setVarCounter(int varCounter) {
        this.varCounter = varCounter;
    }

    @Override
    public LogicalVariable newVar() {
        varCounter++;
        return new LogicalVariable(varCounter);
    }

    @Override
    public LogicalVariable newVar(String displayName) {
        varCounter++;
        return new LogicalVariable(varCounter, displayName);
    }

    @Override
    public IMetadataProvider getMetadataProvider() {
        return metadataProvider;
    }

    @Override
    public void setMetadataDeclarations(IMetadataProvider<?, ?> metadataProvider) {
        this.metadataProvider = metadataProvider;
    }

    @Override
    public boolean checkIfInDontApplySet(IAlgebraicRewriteRule rule, ILogicalOperator op) {
        HashSet<ILogicalOperator> operators = dontApply.get(rule);
        if (operators == null) {
            return false;
        } else {
            return operators.contains(op);
        }
    }

    @Override
    public void addToDontApplySet(IAlgebraicRewriteRule rule, ILogicalOperator op) {
        HashSet<ILogicalOperator> operators = dontApply.get(rule);
        if (operators == null) {
            HashSet<ILogicalOperator> os = new HashSet<>();
            os.add(op);
            dontApply.put(rule, os);
        } else {
            operators.add(op);
        }

    }

    /*
     * returns true if op1 and op2 have already been compared
     */
    @Override
    public boolean checkAndAddToAlreadyCompared(ILogicalOperator op1, ILogicalOperator op2) {
        HashSet<ILogicalOperator> ops = alreadyCompared.get(op1);
        if (ops == null) {
            HashSet<ILogicalOperator> newEntry = new HashSet<>();
            newEntry.add(op2);
            alreadyCompared.put(op1, newEntry);
            return false;
        } else {
            if (ops.contains(op2)) {
                return true;
            } else {
                ops.add(op2);
                return false;
            }
        }
    }

    @Override
    public void removeFromAlreadyCompared(ILogicalOperator op1) {
        alreadyCompared.remove(op1);
    }

    @Override
    public void addNotToBeInlinedVar(LogicalVariable var) {
        notToBeInlinedVars.add(var);
    }

    @Override
    public boolean shouldNotBeInlined(LogicalVariable var) {
        return notToBeInlinedVars.contains(var);
    }

    @Override
    public void addPrimaryKey(FunctionalDependency pk) {
        for (LogicalVariable var : pk.getTail()) {
            varToPrimaryKey.put(var, pk);
        }
    }

    @Override
    public List<LogicalVariable> findPrimaryKey(LogicalVariable recordVar) {
        FunctionalDependency fd = varToPrimaryKey.get(recordVar);
        return fd == null ? null : new ArrayList<>(fd.getHead());
    }

    @Override
    public Map<LogicalVariable, EquivalenceClass> getEquivalenceClassMap(ILogicalOperator op) {
        return eqClassGlobalMap.get(op);
    }

    @Override
    public List<FunctionalDependency> getFDList(ILogicalOperator op) {
        return fdGlobalMap.get(op);
    }

    @Override
    public void putEquivalenceClassMap(ILogicalOperator op, Map<LogicalVariable, EquivalenceClass> eqClassMap) {
        this.eqClassGlobalMap.put(op, eqClassMap);
    }

    @Override
    public void putFDList(ILogicalOperator op, List<FunctionalDependency> fdList) {
        this.fdGlobalMap.put(op, fdList);
    }

    @Override
    public ILogicalPropertiesVector getLogicalPropertiesVector(ILogicalOperator op) {
        return logicalProps.get(op);
    }

    @Override
    public void putLogicalPropertiesVector(ILogicalOperator op, ILogicalPropertiesVector v) {
        logicalProps.put(op, v);
    }

    @Override
    public IExpressionEvalSizeComputer getExpressionEvalSizeComputer() {
        return expressionEvalSizeComputer;
    }

    @Override
    public IVariableEvalSizeEnvironment getVariableEvalSizeEnvironment() {
        return varEvalSizeEnv;
    }

    @Override
    public IMergeAggregationExpressionFactory getMergeAggregationExpressionFactory() {
        return mergeAggregationExpressionFactory;
    }

    @Override
    public PhysicalOptimizationConfig getPhysicalOptimizationConfig() {
        return physicalOptimizationConfig;
    }

    @Override
    public IVariableTypeEnvironment getOutputTypeEnvironment(ILogicalOperator op) {
        return typeEnvMap.get(op);
    }

    @Override
    public void setOutputTypeEnvironment(ILogicalOperator op, IVariableTypeEnvironment env) {
        typeEnvMap.put(op, env);
    }

    @Override
    public IExpressionTypeComputer getExpressionTypeComputer() {
        return expressionTypeComputer;
    }

    @Override
    public IMissableTypeComputer getMissableTypeComputer() {
        return nullableTypeComputer;
    }

    @Override
    public void invalidateTypeEnvironmentForOperator(ILogicalOperator op) {
        typeEnvMap.put(op, null);
    }

    @Override
    public void computeAndSetTypeEnvironmentForOperator(ILogicalOperator op) throws AlgebricksException {
        setOutputTypeEnvironment(op, op.computeOutputTypeEnvironment(this));
    }

    @Override
    public void updatePrimaryKeys(Map<LogicalVariable, LogicalVariable> mappedVars) {
        for (Map.Entry<LogicalVariable, FunctionalDependency> me : varToPrimaryKey.entrySet()) {
            FunctionalDependency fd = me.getValue();
            List<LogicalVariable> hd = new ArrayList<>();
            for (LogicalVariable v : fd.getHead()) {
                LogicalVariable v2 = mappedVars.get(v);
                if (v2 == null) {
                    hd.add(v);
                } else {
                    hd.add(v2);
                }
            }
            List<LogicalVariable> tl = new ArrayList<>();
            for (LogicalVariable v : fd.getTail()) {
                LogicalVariable v2 = mappedVars.get(v);
                if (v2 == null) {
                    tl.add(v);
                } else {
                    tl.add(v2);
                }
            }
            me.setValue(new FunctionalDependency(hd, tl));
        }
    }

    @Override
    public INodeDomain getComputationNodeDomain() {
        return defaultNodeDomain;
    }

    @Override
    public IPlanPrettyPrinter getPrettyPrinter() {
        return prettyPrinter;
    }

    @Override
    public IConflictingTypeResolver getConflictingTypeResolver() {
        return conflictingTypeResovler;
    }

    @Override
    public IWarningCollector getWarningCollector() {
        return warningCollector;
    }
}
