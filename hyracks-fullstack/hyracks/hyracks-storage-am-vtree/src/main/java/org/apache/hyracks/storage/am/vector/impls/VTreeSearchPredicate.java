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
package org.apache.hyracks.storage.am.vector.impls;

import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.ITupleFilter;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.hyracks.storage.common.MultiComparator;

/**
 * Search predicate for vector ANN (Approximate Nearest Neighbor) queries.
 * Holds a reference to the query tuple for per-tuple vector searches.
 *
 * Following the Reference Pattern (like RTree): The predicate holds an ITupleReference
 * that is updated per-tuple by resetSearchPredicate(). The storage layer extracts
 * the query vector when needed during search.
 *
 * For top-K ANN queries, also includes the k parameter (number of nearest neighbors).
 */
public class VTreeSearchPredicate implements ISearchPredicate {
    private static final long serialVersionUID = 1L;

    private ITupleReference queryTuple;
    private int queryFieldIndex;
    private int k; // Number of nearest neighbors to return (for ANN queries)
    private ITupleFilter tupleFilter; // Filter for INCLUDE field predicates (e.g., year > 2000)
    private double minProbeFraction; // Fraction of leaf clusters to probe (0.0-1.0, default 0.1)
    private double epsilon; // Distance threshold for level-wise cross-pollination (internal, not user-facing)
    private int kMultiplier; // Multiplier for candidate limit: K * kMultiplier sent to PK for reranking (default 1)

    private static final double DEFAULT_MIN_PROBE_FRACTION = 0.1;
    private static final double DEFAULT_EPSILON = 0.3;
    private static final int DEFAULT_K_MULTIPLIER = 1;

    public VTreeSearchPredicate() {
        this.k = Integer.MAX_VALUE;
        this.minProbeFraction = DEFAULT_MIN_PROBE_FRACTION;
        this.epsilon = DEFAULT_EPSILON;
        this.kMultiplier = DEFAULT_K_MULTIPLIER;
    }

    public VTreeSearchPredicate(int k) {
        this.k = k;
        this.minProbeFraction = DEFAULT_MIN_PROBE_FRACTION;
        this.epsilon = DEFAULT_EPSILON;
        this.kMultiplier = DEFAULT_K_MULTIPLIER;
    }

    /**
     * Set the query tuple containing the vector field.
     * Called by resetSearchPredicate() for each input tuple.
     */
    public void setQueryTuple(ITupleReference queryTuple) {
        this.queryTuple = queryTuple;
    }

    /**
     * Set the field index of the vector in the query tuple.
     */
    public void setQueryFieldIndex(int queryFieldIndex) {
        this.queryFieldIndex = queryFieldIndex;
    }

    /**
     * Get the query tuple reference.
     */
    public ITupleReference getQueryTuple() {
        return queryTuple;
    }

    /**
     * Get the query field index.
     */
    public int getQueryFieldIndex() {
        return queryFieldIndex;
    }

    /**
     * Set the K parameter (number of nearest neighbors to return).
     */
    public void setK(int k) {
        this.k = k;
    }

    /**
     * Get the K parameter (number of nearest neighbors to return).
     */
    public int getK() {
        return k;
    }

    /**
     * Set the min_probe_fraction parameter (fraction of leaf clusters to probe).
     * Converted to nprobe = max(1, floor(totalLeafClusters * fraction)) at runtime.
     * Value of 0 means use default (0.1).
     */
    public void setMinProbeFraction(double minProbeFraction) {
        if (minProbeFraction <= 0.0) {
            this.minProbeFraction = DEFAULT_MIN_PROBE_FRACTION;
        } else if (minProbeFraction > 1.0) {
            this.minProbeFraction = 1.0;
        } else {
            this.minProbeFraction = minProbeFraction;
        }
    }

    /**
     * Get the min_probe_fraction parameter.
     */
    public double getMinProbeFraction() {
        return minProbeFraction;
    }

    /**
     * Set the epsilon parameter (relative distance threshold for level-wise cross-pollination).
     * Clusters within {@code closestDistance + |closestDistance| * epsilon} — i.e. (1+epsilon)*d
     * for positive distances and (1-epsilon)*d for negative ones — will be explored via level-wise.
     */
    public void setEpsilon(double epsilon) {
        this.epsilon = epsilon;
    }

    /**
     * Get the epsilon parameter (distance threshold for level-wise cross-pollination).
     */
    public double getEpsilon() {
        return epsilon;
    }

    /**
     * Set the tuple filter for INCLUDE field predicates.
     * When set, the cursor will only return tuples that pass this filter,
     * and only count passing tuples toward K.
     */
    public void setTupleFilter(ITupleFilter tupleFilter) {
        this.tupleFilter = tupleFilter;
    }

    /**
     * Get the tuple filter for INCLUDE field predicates.
     */
    public ITupleFilter getTupleFilter() {
        return tupleFilter;
    }

    /**
     * Set the kMultiplier for candidate limit (K * kMultiplier sent to PK for reranking).
     */
    public void setKMultiplier(int kMultiplier) {
        this.kMultiplier = kMultiplier;
    }

    /**
     * Get the kMultiplier for candidate limit.
     */
    public int getKMultiplier() {
        return kMultiplier;
    }

    @Override
    public MultiComparator getLowKeyComparator() {
        // Vector clustering tree doesn't use traditional key comparisons
        return null;
    }

    @Override
    public MultiComparator getHighKeyComparator() {
        // Vector clustering tree doesn't use traditional key comparisons
        return null;
    }

    @Override
    public ITupleReference getLowKey() {
        // Vector clustering tree doesn't use traditional key searches
        return null;
    }

    @Override
    public String toString() {
        return "VTreeSearchPredicate[queryTuple=" + (queryTuple != null ? "set" : "null") + ", k=" + k
                + ", minProbeFraction=" + minProbeFraction + ", epsilon=" + epsilon + ", kMultiplier=" + kMultiplier
                + "]";
    }
}
