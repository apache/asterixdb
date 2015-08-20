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

import java.util.Random;

import org.junit.After;
import org.junit.Before;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import edu.uci.ics.hyracks.storage.am.config.AccessMethodTestsConfig;
import edu.uci.ics.hyracks.storage.am.rtree.frames.RTreePolicyType;
import edu.uci.ics.hyracks.storage.am.rtree.utils.RTreeTestContext;
import edu.uci.ics.hyracks.storage.am.rtree.utils.RTreeTestHarness;

@SuppressWarnings("rawtypes")
public class RTreeDeleteTest extends AbstractRTreeDeleteTest {

	private final RTreeTestHarness harness = new RTreeTestHarness();

	public RTreeDeleteTest() {
		super(AccessMethodTestsConfig.RTREE_TEST_RSTAR_POLICY);
	}
	
    @Before
    public void setUp() throws HyracksDataException {
        harness.setUp();
    }

    @After
    public void tearDown() throws HyracksDataException {
        harness.tearDown();
    }

    @Override
    protected AbstractRTreeTestContext createTestContext(ISerializerDeserializer[] fieldSerdes,
            IPrimitiveValueProviderFactory[] valueProviderFactories, int numKeys, RTreePolicyType rtreePolicyType)
            throws Exception {
        return RTreeTestContext.create(harness.getBufferCache(), harness.getFileMapProvider(),
                harness.getFileReference(), fieldSerdes, valueProviderFactories, numKeys, rtreePolicyType);
    }

    @Override
    protected Random getRandom() {
        return harness.getRandom();
    }
}
