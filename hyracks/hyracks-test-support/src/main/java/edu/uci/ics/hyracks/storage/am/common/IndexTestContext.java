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

package edu.uci.ics.hyracks.storage.am.common;

import java.util.Collection;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IIndex;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;

@SuppressWarnings("rawtypes")
public abstract class IndexTestContext<T extends CheckTuple> implements IIndexTestContext<T> {
    protected final ISerializerDeserializer[] fieldSerdes;
    protected final IIndex index;
    protected final ArrayTupleBuilder tupleBuilder;
    protected final ArrayTupleReference tuple = new ArrayTupleReference();
    protected final IIndexAccessor indexAccessor;

    public IndexTestContext(ISerializerDeserializer[] fieldSerdes, IIndex index) throws HyracksDataException {
        this.fieldSerdes = fieldSerdes;
        this.index = index;
        this.indexAccessor = (IIndexAccessor) index.createAccessor(TestOperationCallback.INSTANCE,
                TestOperationCallback.INSTANCE);
        this.tupleBuilder = new ArrayTupleBuilder(fieldSerdes.length);
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