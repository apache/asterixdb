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

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.IIndex;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;

@SuppressWarnings("rawtypes")
public interface IIndexTestContext<T extends CheckTuple> {
    public int getFieldCount();

    public int getKeyFieldCount();

    public ISerializerDeserializer[] getFieldSerdes();

    public IBinaryComparatorFactory[] getComparatorFactories();

    public IIndexAccessor getIndexAccessor();

    public IIndex getIndex();

    public ArrayTupleReference getTuple();

    public ArrayTupleBuilder getTupleBuilder();

    public void insertCheckTuple(T checkTuple, Collection<T> checkTuples);      

    public void deleteCheckTuple(T checkTuple, Collection<T> checkTuples);

    public Collection<T> getCheckTuples();

}
