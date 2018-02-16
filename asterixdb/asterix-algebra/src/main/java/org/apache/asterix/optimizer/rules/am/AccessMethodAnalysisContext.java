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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Index;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Quadruple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;

/**
 * Context for analyzing the applicability of a single access method.
 */
public class AccessMethodAnalysisContext {

    private List<IOptimizableFuncExpr> matchedFuncExprs = new ArrayList<IOptimizableFuncExpr>();

    // Contains candidate indexes and a list of (integer,integer) tuples that index into matchedFuncExprs and
    // matched variable inside this expr. We are mapping from candidate indexes to a list of function expressions
    // that match one of the index's expressions.
    private Map<Index, List<Pair<Integer, Integer>>> indexExprsAndVars =
            new TreeMap<Index, List<Pair<Integer, Integer>>>();

    // Maps from index to the dataset it is indexing.
    private Map<Index, Dataset> indexDatasetMap = new TreeMap<Index, Dataset>();

    // Maps from an index to the number of matched fields in the query plan (for performing prefix search)
    private Map<Index, Integer> indexNumMatchedKeys = new TreeMap<Index, Integer>();

    // variables for resetting null placeholder for left-outer-join
    private Mutable<ILogicalOperator> lojGroupbyOpRef = null;
    private ScalarFunctionCallExpression lojIsMissingFuncInGroupBy = null;

    // For a secondary index, if we use only PK and secondary key field in a plan, it is an index-only plan.
    // Contains information about index-only plan
    //
    // 1. isIndexOnlyPlan - index-only plan possible?
    //    This option is the primary option. If this is false, then regardless of the following variables,
    //    An index-only plan will not be constructed. If this is true, then we use the following variables to
    //    construct an index-only plan.
    //
    // 2. secondaryKeyFieldUsedAfterSelectOrJoinOp - secondary key field usage after the select or join operator?
    //    If the secondary key field is used after SELECT or JOIN operator (e.g., returning the field),
    //    then we need to keep secondary keys from the secondary index search.
    //
    // 3. requireVerificationAfterSIdxSearch -
    //    whether a verification (especially for R-Tree case) is required after the secondary index search?
    //    For an R-Tree index, if the given query shape is not RECTANGLE or POINT,
    //    we need to add the original SELECT operator to filter out the false positive results.
    //    (e.g., spatial-intersect($o.pointfield, create-circle(create-point(30.0,70.0), 5.0)) )
    //
    //    Also, for a B-Tree composite index, we need to apply SELECT operators in the right path
    //    to remove any false positive results from the secondary composite index search.
    //
    //    Lastly, if there is an index-nested-loop-join and the join contains more conditions
    //    other than joining fields, then those conditions need to be applied to filter out
    //    false positive results in the right path (isntantTryLock success path).
    //    (e.g., where $a.authors /*+ indexnl */ = $b.authors and $a.id = $b.id)
    //    For more details, refer to AccessMethodUtils.createPrimaryIndexUnnestMap() method.
    //
    // 4. doesSIdxSearchCoverAllPredicates - can the given index cover all search predicates?
    //    In other words, all search predicates are about the given secondary index?
    //
    private Quadruple<Boolean, Boolean, Boolean, Boolean> indexOnlyPlanInfo =
            new Quadruple<>(false, false, false, false);

    public void addIndexExpr(Dataset dataset, Index index, Integer exprIndex, Integer varIndex) {
        List<Pair<Integer, Integer>> exprs = getIndexExprsFromIndexExprsAndVars(index);
        if (exprs == null) {
            exprs = new ArrayList<Pair<Integer, Integer>>();
            putIndexExprToIndexExprsAndVars(index, exprs);
        }
        exprs.add(new Pair<Integer, Integer>(exprIndex, varIndex));
        putDatasetIntoIndexDatasetMap(index, dataset);
    }

    public List<IOptimizableFuncExpr> getMatchedFuncExprs() {
        return matchedFuncExprs;
    }

    public IOptimizableFuncExpr getMatchedFuncExpr(int index) {
        return matchedFuncExprs.get(index);
    }

    public void addMatchedFuncExpr(IOptimizableFuncExpr optFuncExpr) {
        matchedFuncExprs.add(optFuncExpr);
    }

    public Iterator<Map.Entry<Index, List<Pair<Integer, Integer>>>> getIteratorForIndexExprsAndVars() {
        return indexExprsAndVars.entrySet().iterator();
    }

    public boolean isIndexExprsAndVarsEmpty() {
        return indexExprsAndVars.isEmpty();
    }

    public List<Pair<Integer, Integer>> getIndexExprsFromIndexExprsAndVars(Index index) {
        return indexExprsAndVars.get(index);
    }

    public void putIndexExprToIndexExprsAndVars(Index index, List<Pair<Integer, Integer>> exprs) {
        indexExprsAndVars.put(index, exprs);
    }

    public Integer getNumberOfMatchedKeys(Index index) {
        return indexNumMatchedKeys.get(index);
    }

    public void putNumberOfMatchedKeys(Index index, Integer numMatchedKeys) {
        indexNumMatchedKeys.put(index, numMatchedKeys);
    }

    public void setLOJGroupbyOpRef(Mutable<ILogicalOperator> opRef) {
        lojGroupbyOpRef = opRef;
    }

    public Mutable<ILogicalOperator> getLOJGroupbyOpRef() {
        return lojGroupbyOpRef;
    }

    public void setLOJIsMissingFuncInGroupBy(ScalarFunctionCallExpression isMissingFunc) {
        lojIsMissingFuncInGroupBy = isMissingFunc;
    }

    public ScalarFunctionCallExpression getLOJIsMissingFuncInGroupBy() {
        return lojIsMissingFuncInGroupBy;
    }

    public Dataset getDatasetFromIndexDatasetMap(Index idx) {
        return getIndexDatasetMap().get(idx);
    }

    public void putDatasetIntoIndexDatasetMap(Index idx, Dataset ds) {
        getIndexDatasetMap().put(idx, ds);
    }

    public void setIndexOnlyPlanInfo(Quadruple<Boolean, Boolean, Boolean, Boolean> indexOnlyPlanInfo) {
        this.indexOnlyPlanInfo = indexOnlyPlanInfo;
    }

    public Quadruple<Boolean, Boolean, Boolean, Boolean> getIndexOnlyPlanInfo() {
        return this.indexOnlyPlanInfo;
    }

    public Map<Index, Dataset> getIndexDatasetMap() {
        return indexDatasetMap;
    }

    public void setIndexDatasetMap(Map<Index, Dataset> indexDatasetMap) {
        this.indexDatasetMap = indexDatasetMap;
    }

}
