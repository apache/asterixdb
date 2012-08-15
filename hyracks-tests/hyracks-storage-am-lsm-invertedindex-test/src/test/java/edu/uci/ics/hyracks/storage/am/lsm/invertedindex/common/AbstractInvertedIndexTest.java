/*
 * Copyright 2009-2012 by The Regents of the University of California
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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.common;

import org.junit.After;
import org.junit.Before;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.LSMInvertedIndexTestHarness;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.util.InvertedIndexTestContext.InvertedIndexType;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;

public abstract class AbstractInvertedIndexTest {
    protected final LSMInvertedIndexTestHarness harness = new LSMInvertedIndexTestHarness();

    protected int NUM_DOCS_TO_INSERT = 10000;    

    protected final InvertedIndexType invIndexType;

    public AbstractInvertedIndexTest(InvertedIndexType invIndexType) {
        this.invIndexType = invIndexType;
    }

    @Before
    public void setUp() throws HyracksException {
        harness.setUp();
    }

    @After
    public void tearDown() throws HyracksDataException {
        harness.tearDown();
    }
    
    public abstract IBufferCache getBufferCache();
}
