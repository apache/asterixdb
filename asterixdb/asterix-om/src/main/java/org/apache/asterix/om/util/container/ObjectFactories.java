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
package org.apache.asterix.om.util.container;

import java.util.BitSet;

import org.apache.asterix.dataflow.data.common.TaggedValueReference;
import org.apache.asterix.om.pointables.nonvisitor.SortedRecord;
import org.apache.asterix.om.types.ARecordType;
import org.apache.hyracks.data.std.api.IMutableValueStorage;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

// TODO(ali): look for all classes creating factories and extract them to here

/**
 * Object factories must be used in conjunction with {@link IObjectPool} to reuse objects. They should not be used
 * to create objects outside the context of a pool.
 */
public class ObjectFactories {

    private ObjectFactories() {
    }

    public static final IObjectFactory<IPointable, Void> VOID_FACTORY = type -> new VoidPointable();
    public static final IObjectFactory<IMutableValueStorage, Void> STORAGE_FACTORY =
            type -> new ArrayBackedValueStorage();
    public static final IObjectFactory<BitSet, Void> BIT_SET_FACTORY = type -> new BitSet();
    public static final IObjectFactory<UTF8StringPointable, Void> UTF8_FACTORY = type -> new UTF8StringPointable();
    public static final IObjectFactory<SortedRecord, ARecordType> RECORD_FACTORY = SortedRecord::new;
    public static final IObjectFactory<TaggedValueReference, Void> VALUE_FACTORY = type -> new TaggedValueReference();
}
