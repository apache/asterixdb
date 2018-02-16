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
package org.apache.asterix.optimizer.rules.am;

import java.util.List;

import org.apache.asterix.metadata.entities.Index;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;

/**
 * Interface that an access method should implement to work with the rewrite
 * rules to apply it for join and/or selection queries. This interface provides
 * methods for analyzing a select/join condition, and for rewriting the plan
 * with a given index.
 */
public interface IAccessMethod extends Comparable<IAccessMethod> {

    /**
     * @return A list of function identifiers that are optimizable by this
     *         access method. Also, the second boolean tells whether that
     *         function can generate a false-positive result.
     */
    public List<Pair<FunctionIdentifier, Boolean>> getOptimizableFunctions();

    /**
     * Analyzes the arguments of a given optimizable funcExpr to see if this
     * access method is applicable (e.g., one arg is a constant and one is a
     * var). We assume that the funcExpr has already been determined to be
     * optimizable by this access method based on its function identifier. If
     * funcExpr has been found to be optimizable, this method adds an
     * OptimizableFunction to analysisCtx.matchedFuncExprs for further analysis.
     *
     * @return true if funcExpr is optimizable by this access method, false
     *         otherwise
     * @throws AlgebricksException
     */
    boolean analyzeFuncExprArgsAndUpdateAnalysisCtx(AbstractFunctionCallExpression funcExpr,
            List<AbstractLogicalOperator> assignsAndUnnests, AccessMethodAnalysisContext analysisCtx,
            IOptimizationContext context, IVariableTypeEnvironment typeEnvironment) throws AlgebricksException;

    /**
     * Indicates whether all index expressions must be matched in order for this
     * index to be applicable.
     *
     * @return boolean
     */
    public boolean matchAllIndexExprs();

    /**
     * Indicates whether this index is applicable if only a prefix of the index
     * expressions are matched.
     *
     * @return boolean
     */
    public boolean matchPrefixIndexExprs();

    /**
     * Applies the plan transformation to use chosenIndex to optimize a selection query.
     *
     * @param afterSelectRefs
     */
    public boolean applySelectPlanTransformation(List<Mutable<ILogicalOperator>> afterSelectRefs,
            Mutable<ILogicalOperator> selectRef, OptimizableOperatorSubTree subTree, Index chosenIndex,
            AccessMethodAnalysisContext analysisCtx, IOptimizationContext context) throws AlgebricksException;

    public ILogicalOperator createIndexSearchPlan(List<Mutable<ILogicalOperator>> afterTopOpRefs,
            Mutable<ILogicalOperator> topOpRef, Mutable<ILogicalExpression> conditionRef,
            List<Mutable<ILogicalOperator>> assignBeforeTheOpRefs, OptimizableOperatorSubTree indexSubTree,
            OptimizableOperatorSubTree probeSubTree, Index chosenIndex, AccessMethodAnalysisContext analysisCtx,
            boolean retainInput, boolean retainNull, boolean requiresBroadcast, IOptimizationContext context,
            LogicalVariable newNullPlaceHolderForLOJ) throws AlgebricksException;

    /**
     * Applies the plan transformation to use chosenIndex to optimize a join query.
     * In the case of a LeftOuterJoin, there may or may not be a needed groupby operation
     * If there is, we will need to include it in the transformation
     */
    public boolean applyJoinPlanTransformation(List<Mutable<ILogicalOperator>> afterJoinRefs,
            Mutable<ILogicalOperator> joinRef, OptimizableOperatorSubTree leftSubTree,
            OptimizableOperatorSubTree rightSubTree, Index chosenIndex, AccessMethodAnalysisContext analysisCtx,
            IOptimizationContext context, boolean isLeftOuterJoin, boolean hasGroupBy) throws AlgebricksException;

    /**
     * Analyzes expr to see whether it is optimizable by the given concrete index.
     *
     * @throws AlgebricksException
     */
    public boolean exprIsOptimizable(Index index, IOptimizableFuncExpr optFuncExpr) throws AlgebricksException;

    public String getName();

}
