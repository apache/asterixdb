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

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.storage.common.IIndex;
import org.apache.hyracks.storage.common.IIndexAccessor;

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
