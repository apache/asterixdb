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

import org.apache.hyracks.storage.am.vector.api.IVTreeDistanceFunction;
import org.apache.hyracks.storage.am.vector.api.IVTreeQuantizer;
import org.apache.hyracks.storage.common.ICursorInitialState;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.ISearchOperationCallback;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;

/**
 * Initial state for vector clustering tree cursors.
 * Contains information needed to position the cursor at the appropriate location.
 */
public class VTreeCursorInitialState implements ICursorInitialState {

    private long metadataPageId;
    private int rootPageId;
    private double[] queryVector;
    private ICachedPage page;
    private ISearchOperationCallback searchCallback;
    private MultiComparator originalKeyCmp;
    private final IIndexAccessor accessor;
    private IVTreeDistanceFunction distanceFunction;
    private double[] quantizedQueryVector;
    private IVTreeQuantizer quantizer;

    public VTreeCursorInitialState(IIndexAccessor accessor) {
        this.accessor = accessor;
        this.metadataPageId = -1;
    }

    public VTreeCursorInitialState(long metadataPageId, double[] queryVector, IIndexAccessor accessor) {
        this.metadataPageId = metadataPageId;
        // No defensive copy: the query vector is effectively immutable for the duration of a search, and the
        // setQueryVector() path stores it by reference too — keep the two consistent.
        this.queryVector = queryVector;
        this.accessor = accessor;
    }

    public IIndexAccessor getIndexAccessor() {
        return accessor;
    }

    public void setMetadataPageId(long metadataPageId) {
        this.metadataPageId = metadataPageId;
    }

    public long getMetadataPageId() {
        return metadataPageId;
    }

    public void setQueryVector(double[] queryVector) {
        this.queryVector = queryVector;
    }

    public double[] getQueryVector() {
        return queryVector;
    }

    public void setRootPageId(int rootPageId) {
        this.rootPageId = rootPageId;
    }

    public int getRootPageId() {
        return rootPageId;
    }

    @Override
    public ICachedPage getPage() {
        return page;
    }

    @Override
    public void setPage(ICachedPage page) {
        this.page = page;
    }

    @Override
    public ISearchOperationCallback getSearchOperationCallback() {
        return searchCallback;
    }

    @Override
    public void setSearchOperationCallback(ISearchOperationCallback searchCallback) {
        this.searchCallback = searchCallback;
    }

    @Override
    public MultiComparator getOriginalKeyComparator() {
        return originalKeyCmp;
    }

    @Override
    public void setOriginialKeyComparator(MultiComparator originalCmp) {
        this.originalKeyCmp = originalCmp;
    }

    @Override
    public String toString() {
        return "VTreeCursorInitialState[metadataPageId=" + metadataPageId + "]";
    }

    public IIndexAccessor getAccessor() {
        return accessor;
    }

    /**
     * Set the distance function for vector distance calculations.
     */
    public void setDistanceFunction(IVTreeDistanceFunction distanceFunction) {
        this.distanceFunction = distanceFunction;
    }

    /**
     * Get the distance function for vector distance calculations.
     */
    public IVTreeDistanceFunction getDistanceFunction() {
        return distanceFunction;
    }

    /**
     * Set the quantized query vector (precomputed once per search).
     */
    public void setQuantizedQueryVector(double[] quantizedQueryVector) {
        this.quantizedQueryVector = quantizedQueryVector;
    }

    /**
     * Get the quantized query vector, or null if quantization is not configured.
     */
    public double[] getQuantizedQueryVector() {
        return quantizedQueryVector;
    }

    /**
     * Set the vector quantizer.
     */
    public void setQuantizer(IVTreeQuantizer quantizer) {
        this.quantizer = quantizer;
    }

    /**
     * Get the vector quantizer, or null if quantization is not configured.
     */
    public IVTreeQuantizer getQuantizer() {
        return quantizer;
    }
}
