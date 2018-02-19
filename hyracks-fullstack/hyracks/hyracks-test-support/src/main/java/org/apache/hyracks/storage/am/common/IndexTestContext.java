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

package org.apache.hyracks.storage.am.common;

import java.util.Collection;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.IIndexAccessor;

@SuppressWarnings("rawtypes")
public abstract class IndexTestContext<T extends CheckTuple> implements IIndexTestContext<T> {
    protected final ISerializerDeserializer[] fieldSerdes;
    protected final IIndex index;
    protected final ArrayTupleBuilder tupleBuilder;
    protected final ArrayTupleReference tuple = new ArrayTupleReference();
    protected IIndexAccessor indexAccessor;

    public IndexTestContext(ISerializerDeserializer[] fieldSerdes, IIndex index, boolean filtered)
            throws HyracksDataException {
        this.fieldSerdes = fieldSerdes;
        this.index = index;
        IndexAccessParameters actx =
                new IndexAccessParameters(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE);
        this.indexAccessor = index.createAccessor(actx);
        this.tupleBuilder =
                filtered ? new ArrayTupleBuilder(fieldSerdes.length + 1) : new ArrayTupleBuilder(fieldSerdes.length);
    }

    @Override
    public int getFieldCount() {
        return fieldSerdes.length;
    }

    @Override
    public IIndexAccessor getIndexAccessor() {
        return indexAccessor;
    }

    @Override
    public ArrayTupleReference getTuple() {
        return tuple;
    }

    @Override
    public ArrayTupleBuilder getTupleBuilder() {
        return tupleBuilder;
    }

    @Override
    public ISerializerDeserializer[] getFieldSerdes() {
        return fieldSerdes;
    }

    @Override
    public IIndex getIndex() {
        return index;
    }

    @Override
    public void insertCheckTuple(T checkTuple, Collection<T> checkTuples) {
        checkTuples.add(checkTuple);
    }

    @Override
    public void deleteCheckTuple(T checkTuple, Collection<T> checkTuples) {
        checkTuples.remove(checkTuple);
    }
}
