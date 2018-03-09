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
package org.apache.hyracks.storage.am.btree;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.common.api.IExtendedModificationOperationCallback;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.config.AccessMethodTestsConfig;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IModificationOperationCallback;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void modificationCallbackTest() throws Exception {
        IndexAccessParameters actx = new IndexAccessParameters(cb, NoOpOperationCallback.INSTANCE);
        IIndexAccessor accessor = index.createAccessor(actx);

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

    private class VeriyfingModificationCallback implements IExtendedModificationOperationCallback {

        @Override
        public void before(ITupleReference tuple) throws HyracksDataException {
            Assert.assertEquals(0, cmp.compare(AbstractModificationOperationCallbackTest.this.tuple, tuple));
        }

        @Override
        public void found(ITupleReference before, ITupleReference after) throws HyracksDataException {
            if (isFoundNull) {
                Assert.assertEquals(null, before);
            } else {
                Assert.assertEquals(0, cmp.compare(AbstractModificationOperationCallbackTest.this.tuple, before));
            }
            Assert.assertEquals(0, cmp.compare(AbstractModificationOperationCallbackTest.this.tuple, after));
        }

        @Override
        public void after(ITupleReference tuple) throws HyracksDataException {
            Assert.assertEquals(0, cmp.compare(AbstractModificationOperationCallbackTest.this.tuple, tuple));
        }
    }

}
