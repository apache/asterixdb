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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.util.TupleUtils;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IModificationOperationCallback;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.config.AccessMethodTestsConfig;

public abstract class AbstractModificationOperationCallbackTest extends AbstractOperationCallbackTest {

    protected final ArrayTupleBuilder builder;
    protected final ArrayTupleReference tuple;
    protected final IModificationOperationCallback cb;

    protected boolean isFoundNull;

    public AbstractModificationOperationCallbackTest() {
        this.builder = new ArrayTupleBuilder(NUM_KEY_FIELDS);
        this.tuple = new ArrayTupleReference();
        this.cb = new VeriyfingModificationCallback();
        this.isFoundNull = true;
    }

    @Before
    public void setup() throws Exception {
        super.setup();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void modificationCallbackTest() throws Exception {
        IIndexAccessor accessor = index.createAccessor(cb, NoOpOperationCallback.INSTANCE);

        isFoundNull = true;
        for (int i = 0; i < AccessMethodTestsConfig.BTREE_NUM_TUPLES_TO_INSERT; i++) {
            TupleUtils.createIntegerTuple(builder, tuple, i);
            accessor.insert(tuple);
        }

        isFoundNull = false;
        for (int i = 0; i < AccessMethodTestsConfig.BTREE_NUM_TUPLES_TO_INSERT; i++) {
            TupleUtils.createIntegerTuple(builder, tuple, i);
            accessor.upsert(tuple);
        }

        isFoundNull = false;
        for (int i = 0; i < AccessMethodTestsConfig.BTREE_NUM_TUPLES_TO_INSERT; i++) {
            TupleUtils.createIntegerTuple(builder, tuple, i);
            accessor.delete(tuple);
        }
    }

    private class VeriyfingModificationCallback implements IModificationOperationCallback {

        @Override
        public void before(ITupleReference tuple) {
            Assert.assertEquals(0, cmp.compare(AbstractModificationOperationCallbackTest.this.tuple, tuple));
        }

        @Override
        public void found(ITupleReference before, ITupleReference after) {
            if (isFoundNull) {
                Assert.assertEquals(null, before);
            } else {
                Assert.assertEquals(0, cmp.compare(AbstractModificationOperationCallbackTest.this.tuple, before));
            }
            Assert.assertEquals(0, cmp.compare(AbstractModificationOperationCallbackTest.this.tuple, after));
        }

    }

}
