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
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Transforms input tuples from the operator format into the storage format
 * used in VTree data pages. The output format is implementation-specific
 * (e.g., with or without quantized vector embeddings).
 *
 * Implementations are created by {@link IVTreeDataTupleBuilderFactory} and
 * held by the operation context for the lifetime of an index accessor.
 */
public interface IVTreeDataTupleBuilder {

    /**
     * Create a data tuple for VTree storage.
     *
     * @param vector the vector extracted from the input tuple
     * @param distance the computed distance from the vector to its assigned centroid
     * @param centroidId the ID of the assigned leaf centroid
     * @param originalTuple the original input tuple containing vector, include fields, and primary key
     * @return ITupleReference in storage format (valid until the next call)
     * @throws HyracksDataException if tuple creation fails
     */
    ITupleReference buildDataTuple(double[] vector, double distance, int centroidId, ITupleReference originalTuple)
            throws HyracksDataException;

}
