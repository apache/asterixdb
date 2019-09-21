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

import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionEvalSizeComputer;
import org.apache.hyracks.algebricks.core.algebra.expressions.IMergeAggregationExpressionFactory;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableEvalSizeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.IPlanPrettyPrinter;
import org.apache.hyracks.algebricks.core.algebra.properties.FunctionalDependency;
import org.apache.hyracks.algebricks.core.algebra.properties.ILogicalPropertiesVector;
import org.apache.hyracks.algebricks.core.algebra.properties.INodeDomain;
import org.apache.hyracks.algebricks.core.algebra.typing.ITypingContext;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import org.apache.hyracks.api.exceptions.IWarningCollector;

public interface IOptimizationContext extends ITypingContext, IVariableContext {

    @Override
    public IMetadataProvider<?, ?> getMetadataProvider();

    public void setMetadataDeclarations(IMetadataProvider<?, ?> metadataProvider);

    public boolean checkIfInDontApplySet(IAlgebraicRewriteRule rule, ILogicalOperator op);

    public void addToDontApplySet(IAlgebraicRewriteRule rule, ILogicalOperator op);

    /*
     * returns true if op1 and op2 have already been compared
     */
    public boolean checkAndAddToAlreadyCompared(ILogicalOperator op1, ILogicalOperator op2);

    public void removeFromAlreadyCompared(ILogicalOperator op1);

    public void addNotToBeInlinedVar(LogicalVariable var);

    public boolean shouldNotBeInlined(LogicalVariable var);

    public void addPrimaryKey(FunctionalDependency pk);

    public List<LogicalVariable> findPrimaryKey(LogicalVariable var);

    public void putEquivalenceClassMap(ILogicalOperator op, Map<LogicalVariable, EquivalenceClass> eqClassMap);

    public Map<LogicalVariable, EquivalenceClass> getEquivalenceClassMap(ILogicalOperator op);

    public void putFDList(ILogicalOperator op, List<FunctionalDependency> fdList);

    public List<FunctionalDependency> getFDList(ILogicalOperator op);

    public void putLogicalPropertiesVector(ILogicalOperator op, ILogicalPropertiesVector v);

    public ILogicalPropertiesVector getLogicalPropertiesVector(ILogicalOperator op);

    public IExpressionEvalSizeComputer getExpressionEvalSizeComputer();

    public IVariableEvalSizeEnvironment getVariableEvalSizeEnvironment();

    public IMergeAggregationExpressionFactory getMergeAggregationExpressionFactory();

    public PhysicalOptimizationConfig getPhysicalOptimizationConfig();

    public void updatePrimaryKeys(Map<LogicalVariable, LogicalVariable> mappedVars);

    public IPlanPrettyPrinter getPrettyPrinter();

    public INodeDomain getComputationNodeDomain();

    public IWarningCollector getWarningCollector();
}
