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

package org.apache.asterix.fuzzyjoin.similarity;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ISequenceIterator;

public interface IGenericSimilarityMetric {
    /**
     * Returns the similarity value for the given two lists.
     *
     * @param firstSequence
     *            an instance of {@link org.apache.hyracks.data.std.util.ISequenceIterator}
     * @param secondSequence
     *            an instance of {@link org.apache.hyracks.data.std.util.ISequenceIterator}
     * @return a float similarity value
     * @throws HyracksDataException
     */
    public float computeSimilarity(ISequenceIterator firstSequence, ISequenceIterator secondSequence)
            throws HyracksDataException;

    /**
     * Returns the similarity value for the given two lists. If the calculated similarity value
     * doesn't satisfy the given simThresh value based on the function's check condition, this returns -1.
     *
     * @param firstSequence
     *            an instance of {@link org.apache.hyracks.data.std.util.ISequenceIterator}
     * @param secondSequence
     *            an instance of {@link org.apache.hyracks.data.std.util.ISequenceIterator}
     * @return a float similarity value.
     * @throws HyracksDataException
     */
    public float computeSimilarity(ISequenceIterator firstSequence, ISequenceIterator secondSequence, float simThresh)
            throws HyracksDataException;
}
