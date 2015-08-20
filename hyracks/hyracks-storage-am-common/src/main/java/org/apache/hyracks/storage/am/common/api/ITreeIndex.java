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

package edu.uci.ics.hyracks.storage.am.common.api;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;

/**
 * Interface describing the operations of tree-based index structures. Indexes
 * implementing this interface can easily reuse the tree index operators for
 * dataflow. We assume that indexes store tuples with a fixed number of fields.
 * Users must perform operations on an ITreeIndex via an ITreeIndexAccessor.
 */
public interface ITreeIndex extends IIndex {
    /**
     * @return The index's leaf frame factory.
     */
    public ITreeIndexFrameFactory getLeafFrameFactory();

    /**
     * @return The index's interior frame factory.
     */
    public ITreeIndexFrameFactory getInteriorFrameFactory();

    /**
     * @return The index's free page manager.
     */
    public IFreePageManager getFreePageManager();

    /**
     * @return The number of fields tuples of this index have.
     */
    public int getFieldCount();

    /**
     * @return The current root page id of this index.
     */
    public int getRootPageId();

    /**
     * @return The file id of this index.
     */
    public int getFileId();

    /**
     * @return Comparator factories.
     */
    public IBinaryComparatorFactory[] getComparatorFactories();
}
