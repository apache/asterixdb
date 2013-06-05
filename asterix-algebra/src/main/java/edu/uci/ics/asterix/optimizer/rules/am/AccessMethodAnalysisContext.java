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
import java.util.HashMap;
import java.util.List;

import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.Index;

/**
 * Context for analyzing the applicability of a single access method.
 */
public class AccessMethodAnalysisContext {

    public List<IOptimizableFuncExpr> matchedFuncExprs = new ArrayList<IOptimizableFuncExpr>();

    // Contains candidate indexes and a list of integers that index into matchedFuncExprs.
    // We are mapping from candidate indexes to a list of function expressions 
    // that match one of the index's expressions.
    public HashMap<Index, List<Integer>> indexExprs = new HashMap<Index, List<Integer>>();

    // Maps from index to the dataset it is indexing.
    public HashMap<Index, Dataset> indexDatasetMap = new HashMap<Index, Dataset>();

    public void addIndexExpr(Dataset dataset, Index index, Integer exprIndex) {
        List<Integer> exprs = indexExprs.get(index);
        if (exprs == null) {
            exprs = new ArrayList<Integer>();
            indexExprs.put(index, exprs);
        }
        exprs.add(exprIndex);
        indexDatasetMap.put(index, dataset);
    }

    public List<Integer> getIndexExprs(Index index) {
        return indexExprs.get(index);
    }
}
