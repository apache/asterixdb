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
package edu.uci.ics.asterix.optimizer.rules.am;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.Index;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;

/**
 * Context for analyzing the applicability of a single access method.
 */
public class AccessMethodAnalysisContext {

    public List<IOptimizableFuncExpr> matchedFuncExprs = new ArrayList<IOptimizableFuncExpr>();

    // Contains candidate indexes and a list of (integer,integer) tuples that index into matchedFuncExprs and matched variable inside this expr.
    // We are mapping from candidate indexes to a list of function expressions 
    // that match one of the index's expressions.
    public Map<Index, List<Pair<Integer, Integer>>> indexExprsAndVars = new TreeMap<Index, List<Pair<Integer, Integer>>>();

    // Maps from index to the dataset it is indexing.
    public Map<Index, Dataset> indexDatasetMap = new TreeMap<Index, Dataset>();
    
    // Maps from an index to the number of matched fields in the query plan (for performing prefix search)
    public Map<Index, Integer> indexNumMatchedKeys = new TreeMap<Index, Integer>();

    // variables for resetting null placeholder for left-outer-join
    private Mutable<ILogicalOperator> lojGroupbyOpRef = null;
    private ScalarFunctionCallExpression lojIsNullFuncInGroupBy = null;

    public void addIndexExpr(Dataset dataset, Index index, Integer exprIndex, Integer varIndex) {
        List<Pair<Integer, Integer>> exprs = indexExprsAndVars.get(index);
        if (exprs == null) {
            exprs = new ArrayList<Pair<Integer, Integer>>();
            indexExprsAndVars.put(index, exprs);
        }
        exprs.add(new Pair<Integer, Integer>(exprIndex, varIndex));
        indexDatasetMap.put(index, dataset);
    }

    public List<Pair<Integer, Integer>> getIndexExprs(Index index) {
        return indexExprsAndVars.get(index);
    }

    public void setLOJGroupbyOpRef(Mutable<ILogicalOperator> opRef) {
        lojGroupbyOpRef = opRef;
    }

    public Mutable<ILogicalOperator> getLOJGroupbyOpRef() {
        return lojGroupbyOpRef;
    }

    public void setLOJIsNullFuncInGroupBy(ScalarFunctionCallExpression isNullFunc) {
        lojIsNullFuncInGroupBy = isNullFunc;
    }

    public ScalarFunctionCallExpression getLOJIsNullFuncInGroupBy() {
        return lojIsNullFuncInGroupBy;
    }

}
