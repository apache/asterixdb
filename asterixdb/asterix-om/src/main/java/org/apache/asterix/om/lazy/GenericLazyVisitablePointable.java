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
package org.apache.asterix.om.lazy;

import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * This is a generic lazy visitable-pointable for tagged values (a.k.a. open values). Each nested visitable-pointable
 * should only allocate a single instance of this class and reuse it for every open value.
 */
public class GenericLazyVisitablePointable extends AbstractLazyVisitablePointable {
    private RecordLazyVisitablePointable object;
    private VariableListLazyVisitablePointable array;
    private VariableListLazyVisitablePointable multiset;
    private FlatLazyVisitablePointable flat;

    private AbstractLazyVisitablePointable current;

    public GenericLazyVisitablePointable() {
        super(true);
    }

    @Override
    public final byte getSerializedTypeTag() {
        return current.getSerializedTypeTag();
    }

    @Override
    public final ATypeTag getTypeTag() {
        return current.getTypeTag();
    }

    @Override
    public <R, T> R accept(ILazyVisitablePointableVisitor<R, T> visitor, T arg) throws HyracksDataException {
        switch (current.getTypeTag()) {
            case OBJECT:
                return visitor.visit(object, arg);
            case ARRAY:
                return visitor.visit(array, arg);
            case MULTISET:
                return visitor.visit(multiset, arg);
            default:
                return visitor.visit(flat, arg);
        }
    }

    @Override
    void init(byte[] data, int offset, int length) {
        ATypeTag typeTag = ATypeTag.VALUE_TYPE_MAPPING[data[offset]];
        AbstractLazyVisitablePointable visitable = getOrCreateVisitablePointable(typeTag);
        visitable.set(data, offset, length);
        current = visitable;
    }

    private AbstractLazyVisitablePointable getOrCreateVisitablePointable(ATypeTag typeTag) {
        switch (typeTag) {
            case OBJECT:
                object = object == null ? new RecordLazyVisitablePointable(true) : object;
                return object;
            case ARRAY:
                array = array == null ? new VariableListLazyVisitablePointable(true,
                        DefaultOpenFieldType.NESTED_OPEN_AORDERED_LIST_TYPE) : array;
                return array;
            case MULTISET:
                multiset = multiset == null ? new VariableListLazyVisitablePointable(true,
                        DefaultOpenFieldType.NESTED_OPEN_AUNORDERED_LIST_TYPE) : multiset;
                return multiset;
            default:
                flat = flat == null ? new FlatLazyVisitablePointable(true, ATypeTag.ANY) : flat;
                return flat;
        }
    }
}
