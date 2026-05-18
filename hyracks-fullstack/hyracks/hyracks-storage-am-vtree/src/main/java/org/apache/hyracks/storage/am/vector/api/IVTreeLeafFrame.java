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
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;

/**
 * Interface for VTree leaf frames.
 * Leaf frames contain cluster entries: <cid, full_precision_centroid, pointer_to_first_metadata_page>
 */
public interface IVTreeLeafFrame extends IVTreeFrame {

    void setNextLeaf(int nextLeafPage);

    int getNextLeaf();

    int getMetadataPagePointer(int tupleIndex) throws HyracksDataException;

    /**
     * Sets the metadata page pointer for a cluster entry.
     * @param tupleIndex the index of the cluster entry
     * @param metadataPageId the page ID of the first metadata page
     * @throws HyracksDataException if an error occurs
     */
    void setMetadataPagePointer(int tupleIndex, int metadataPageId) throws HyracksDataException;

    int getCentroidId(int tupleIndex) throws HyracksDataException;

    /**
     * Inserts a cluster entry at the specified index.
     * @param tuple the cluster entry to insert
     * @param tupleIndex the index to insert at
     */
    void insert(ITupleReference tuple, int tupleIndex);

    boolean getOverflowFlagBit();

    void setOverflowFlagBit(boolean overflowFlag);

    /**
     * Gets the quantized centroid bytes for a cluster entry, if present.
     * For quantized tuples (4-field format), returns the byte[] at field 2.
     * For non-quantized tuples (3-field format), returns null.
     *
     * @param tupleIndex the index of the cluster entry
     * @return the quantized centroid bytes, or null if not quantized
     * @throws HyracksDataException if an error occurs
     */
    byte[] getQuantizedCentroidBytes(int tupleIndex) throws HyracksDataException;

    /**
     * Returns the frame's reusable, shared tuple reference. It is positional scratch state: reset it
     * ({@code resetByTupleIndex}/{@code resetByTupleOffset}) before reading, and do not retain a
     * position across another call that also drives it (e.g. {@link #getCentroidId}).
     */
    ITreeIndexTupleReference getFrameTuple();
}
