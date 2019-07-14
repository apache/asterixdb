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
package org.apache.asterix.om.pointables.nonvisitor;

import java.util.Comparator;

import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;

/**
 * This class can be used to hold information about a field in a record. The {@link #index} variable is the position of
 * the field in the record as laid out in the bytes of the record. The value pointed to by the {@link #valueOffset}
 * could be tagged or non-tagged.
 */
public final class RecordField {

    public static final Comparator<RecordField> FIELD_NAME_COMP =
            (field1, field2) -> UTF8StringPointable.compare(field1.namePointable, field2.namePointable);
    private UTF8StringPointable namePointable;
    private ATypeTag valueTag;
    private int index;
    private int valueOffset;
    private int valueLength;

    RecordField() {
    }

    // for open field
    final void set(UTF8StringPointable namePointable, int index, int valueOffset, int valueLength, ATypeTag valueTag) {
        this.namePointable = namePointable;
        set(index, valueOffset, valueLength, valueTag);
    }

    // for closed field where the name is already set
    final void set(int index, int valueOffset, int valueLength, ATypeTag valueTag) {
        this.index = index;
        this.valueOffset = valueOffset;
        this.valueTag = valueTag;
        this.valueLength = valueLength;
    }

    public final UTF8StringPointable getName() {
        return namePointable;
    }

    public final int getIndex() {
        return index;
    }

    final int getValueOffset() {
        return valueOffset;
    }

    final int getValueLength() {
        return valueLength;
    }

    final ATypeTag getValueTag() {
        return valueTag;
    }
}
