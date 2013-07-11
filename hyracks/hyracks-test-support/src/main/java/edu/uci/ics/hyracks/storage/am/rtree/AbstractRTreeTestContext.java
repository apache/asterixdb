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

package edu.uci.ics.hyracks.storage.am.rtree;

import java.util.Collection;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.IndexTestContext;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.util.HashMultiSet;

@SuppressWarnings("rawtypes")
public abstract class AbstractRTreeTestContext extends IndexTestContext<RTreeCheckTuple> {
    private final HashMultiSet<RTreeCheckTuple> checkTuples = new HashMultiSet<RTreeCheckTuple>();

    public AbstractRTreeTestContext(ISerializerDeserializer[] fieldSerdes, ITreeIndex treeIndex)
            throws HyracksDataException {
        super(fieldSerdes, treeIndex);
    }

    @Override
    public Collection<RTreeCheckTuple> getCheckTuples() {
        return checkTuples;
    }
}