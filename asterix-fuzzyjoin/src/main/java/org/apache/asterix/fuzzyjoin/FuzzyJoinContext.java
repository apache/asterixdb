/**
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

package org.apache.asterix.fuzzyjoin;

import java.util.ArrayList;

import org.apache.asterix.fuzzyjoin.similarity.SimilarityFiltersJaccard;

public class FuzzyJoinContext {
    public final float similarityThreshold;
    public final SimilarityFiltersJaccard similarityFilters;
    public final ArrayList<int[]> records;
    public final ArrayList<ResultSelfJoin> results;

    public FuzzyJoinContext(float similarityThreshold) {
        this.similarityThreshold = similarityThreshold;
        similarityFilters = new SimilarityFiltersJaccard(similarityThreshold);
        records = new ArrayList<int[]>();
        results = new ArrayList<ResultSelfJoin>();
    }
}
