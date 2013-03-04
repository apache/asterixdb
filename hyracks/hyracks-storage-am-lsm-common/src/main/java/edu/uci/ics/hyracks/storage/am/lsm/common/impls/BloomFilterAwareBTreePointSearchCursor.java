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

package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.bloomfilter.impls.BloomFilter;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;

public class BloomFilterAwareBTreePointSearchCursor extends BTreeRangeSearchCursor {
    private final BloomFilter bloomFilter;
    private long[] hashes = new long[2];

    public BloomFilterAwareBTreePointSearchCursor(IBTreeLeafFrame frame, boolean exclusiveLatchNodes,
            BloomFilter bloomFilter) {
        super(frame, exclusiveLatchNodes);
        this.bloomFilter = bloomFilter;
    }

    @Override
    public boolean hasNext() throws HyracksDataException {
        if (bloomFilter.contains(lowKey, hashes)) {
            return super.hasNext();
        }
        return false;
    }
}