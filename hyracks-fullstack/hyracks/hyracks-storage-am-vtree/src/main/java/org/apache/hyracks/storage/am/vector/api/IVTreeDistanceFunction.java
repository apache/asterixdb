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
package org.apache.hyracks.storage.am.vector.api;

import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Distance function between two decoded vectors, injected into the VTree index by the Hyracks
 * application that creates it (e.g. Euclidean, cosine, negated dot product).
 * <p>
 * Unlike {@link org.apache.hyracks.api.dataflow.value.IBinaryComparator}, which returns a
 * three-valued ordering (-1/0/+1) over raw field bytes, this returns a real-valued magnitude over
 * two already-decoded {@code double[]} vectors: smaller means nearer, and the values are compared
 * numerically to rank candidates.
 */
@FunctionalInterface
public interface IVTreeDistanceFunction {

    /**
     * @return the distance between {@code vector1} and {@code vector2}.
     * @throws HyracksDataException if the calculation fails
     */
    double apply(double[] vector1, double[] vector2) throws HyracksDataException;
}
