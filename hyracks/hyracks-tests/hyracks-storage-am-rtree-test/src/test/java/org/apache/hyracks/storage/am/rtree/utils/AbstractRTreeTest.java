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

package edu.uci.ics.hyracks.storage.am.rtree.utils;

import java.util.logging.Logger;

import org.junit.After;
import org.junit.Before;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public abstract class AbstractRTreeTest {
    protected final Logger LOGGER = Logger.getLogger(RTreeTestHarness.class.getName());
    protected final RTreeTestHarness harness;

    public AbstractRTreeTest() {
        harness = new RTreeTestHarness();
    }

    public AbstractRTreeTest(int pageSize, int numPages, int maxOpenFiles, int hyracksFrameSize) {
        harness = new RTreeTestHarness(pageSize, numPages, maxOpenFiles, hyracksFrameSize);
    }

    @Before
    public void setUp() throws HyracksDataException {
        harness.setUp();
    }

    @After
    public void tearDown() throws HyracksDataException {
        harness.tearDown();
    }
}
