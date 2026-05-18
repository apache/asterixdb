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
package org.apache.hyracks.storage.am.lsm.vector.impls;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.common.projection.ITupleProjector;
import org.apache.hyracks.storage.common.projection.ITupleProjectorFactory;

/**
 * Factory for creating PKOnlyTupleProjector instances.
 *
 * Used by vector index search operators to create projectors that extract only
 * primary key fields from vector index search results, skipping secondary key fields
 * (distance, cosine similarity, embedding vector).
 */
public class PKOnlyTupleProjectorFactory implements ITupleProjectorFactory {

    private static final long serialVersionUID = 1L;

    private final int numSecondaryKeys; // Number of fields to skip
    private final int numPrimaryKeys; // Number of PK fields to write

    public PKOnlyTupleProjectorFactory(int numSecondaryKeys, int numPrimaryKeys) {
        this.numSecondaryKeys = numSecondaryKeys;
        this.numPrimaryKeys = numPrimaryKeys;
    }

    @Override
    public ITupleProjector createTupleProjector(IHyracksTaskContext context) throws HyracksDataException {
        return new PKOnlyTupleProjector(numSecondaryKeys, numPrimaryKeys);
    }
}
