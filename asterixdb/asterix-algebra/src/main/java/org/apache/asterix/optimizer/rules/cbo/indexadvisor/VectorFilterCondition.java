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
package org.apache.asterix.optimizer.rules.cbo.indexadvisor;

import java.util.List;

import org.apache.asterix.common.vector.VectorSimilarityMetric;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;

/**
 * An approximate-nearest-neighbour search found in the query, described in the terms a vector index
 * candidate needs: which field is searched, under which similarity metric, and at which dimension.
 *
 * @param scanVar variable the searched field is accessed from
 * @param fieldPath path of the searched vector field
 * @param similarity similarity metric the search requests
 * @param dimension number of components of the query vector
 */
public record VectorFilterCondition(LogicalVariable scanVar, List<String> fieldPath, VectorSimilarityMetric similarity,
        int dimension) {
}
