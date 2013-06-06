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
package edu.uci.ics.hyracks.storage.am.btree;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.util.SerdeUtils;
import edu.uci.ics.hyracks.storage.am.common.api.IIndex;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;

public abstract class AbstractOperationCallbackTest {
    protected static final int NUM_KEY_FIELDS = 1;

    @SuppressWarnings("rawtypes")
    protected final ISerializerDeserializer[] keySerdes;
    protected final MultiComparator cmp;
    protected final int[] bloomFilterKeyFields;

    protected IIndex index;

    protected abstract void createIndexInstance() throws Exception;

    public AbstractOperationCallbackTest() {
        this.keySerdes = new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE };
        this.cmp = MultiComparator.create(SerdeUtils.serdesToComparatorFactories(keySerdes, keySerdes.length));
        bloomFilterKeyFields = new int[NUM_KEY_FIELDS];
        for (int i = 0; i < NUM_KEY_FIELDS; ++i) {
            bloomFilterKeyFields[i] = i;
        }
    }

    public void setup() throws Exception {
        createIndexInstance();
        index.create();
        index.activate();
    }

    public void tearDown() throws Exception {
        index.deactivate();
        index.destroy();
    }
}
