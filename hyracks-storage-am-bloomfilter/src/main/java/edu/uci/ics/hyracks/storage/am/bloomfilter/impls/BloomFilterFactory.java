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

package edu.uci.ics.hyracks.storage.am.bloomfilter.impls;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.file.IFileMapProvider;

public class BloomFilterFactory {
    private final IBufferCache bufferCache;
    private final IFileMapProvider fileMapProvider;
    private final int[] bloomFilterKeyFields;

    public BloomFilterFactory(IBufferCache bufferCache, IFileMapProvider fileMapProvider, int[] bloomFilterKeyFields) {
        this.bufferCache = bufferCache;
        this.fileMapProvider = fileMapProvider;
        this.bloomFilterKeyFields = bloomFilterKeyFields;
    }

    public BloomFilter createBloomFiltertInstance(FileReference file) throws HyracksDataException {
        return new BloomFilter(bufferCache, fileMapProvider, file, bloomFilterKeyFields);
    }

    public int[] getBloomFilterKeyFields() {
        return bloomFilterKeyFields;
    }
}
