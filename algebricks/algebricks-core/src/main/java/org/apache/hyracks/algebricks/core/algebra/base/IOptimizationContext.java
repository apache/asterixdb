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
package org.apache.hyracks.algebricks.core.algebra.base;

import java.util.List;
import java.util.Map;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionEvalSizeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.IMergeAggregationExpressionFactory;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableEvalSizeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.LogicalOperatorPrettyPrintVisitor;
import org.apache.hyracks.algebricks.core.algebra.properties.FunctionalDependency;
import org.apache.hyracks.algebricks.core.algebra.properties.ILogicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;

public interface IOptimizationContext extends ITypingContext {

    public abstract int getVarCounter();

    public abstract void setVarCounter(int varCounter);

    public abstract LogicalVariable newVar();

    public abstract IMetadataProvider<?, ?> getMetadataProvider();

    public abstract void setMetadataDeclarations(IMetadataProvider<?, ?> metadataProvider);

    public abstract boolean checkIfInDontApplySet(IAlgebraicRewriteRule rule, ILogicalOperator op);

    public abstract void addToDontApplySet(IAlgebraicRewriteRule rule, ILogicalOperator op);

    /*
     * returns true if op1 and op2 have already been compared
     */
    public abstract boolean checkAndAddToAlreadyCompared(ILogicalOperator op1, ILogicalOperator op2);
    
    public abstract void removeFromAlreadyCompared(ILogicalOperator op1);

    public abstract void addNotToBeInlinedVar(LogicalVariable var);

    public abstract boolean shouldNotBeInlined(LogicalVariable var);

    public abstract void addPrimaryKey(FunctionalDependency pk);

    public abstract List<LogicalVariable> findPrimaryKey(LogicalVariable recordVar);

    public abstract void putEquivalenceClassMap(ILogicalOperator op, Map<LogicalVariable, EquivalenceClass> eqClassMap);

    public abstract Map<LogicalVariable, EquivalenceClass> getEquivalenceClassMap(ILogicalOperator op);

    public abstract void putFDList(ILogicalOperator op, List<FunctionalDependency> fdList);

    public abstract List<FunctionalDependency> getFDList(ILogicalOperator op);

    public abstract void putLogicalPropertiesVector(ILogicalOperator op, ILogicalPropertiesVector v);

    public abstract ILogicalPropertiesVector getLogicalPropertiesVector(ILogicalOperator op);

    public abstract IExpressionEvalSizeComputer getExpressionEvalSizeComputer();

    public abstract IVariableEvalSizeEnvironment getVariableEvalSizeEnvironment();

    public abstract IMergeAggregationExpressionFactory getMergeAggregationExpressionFactory();

    public abstract PhysicalOptimizationConfig getPhysicalOptimizationConfig();

    public abstract void invalidateTypeEnvironmentForOperator(ILogicalOperator op);

    public abstract void computeAndSetTypeEnvironmentForOperator(ILogicalOperator op) throws AlgebricksException;

    public abstract void updatePrimaryKeys(Map<LogicalVariable, LogicalVariable> mappedVars);
    
    public abstract LogicalOperatorPrettyPrintVisitor getPrettyPrintVisitor();
}
