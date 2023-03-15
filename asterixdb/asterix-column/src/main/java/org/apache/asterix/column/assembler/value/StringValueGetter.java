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
package org.apache.asterix.column.assembler.value;

import org.apache.asterix.column.values.IColumnValuesReader;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

class StringValueGetter implements IValueGetter {
    private final ArrayBackedValueStorage value;

    public StringValueGetter() {
        value = new ArrayBackedValueStorage();
    }

    @Override
    public IValueReference getValue(IColumnValuesReader reader) {
        IValueReference string = reader.getBytes();
        value.setSize(1 + string.getLength());
        byte[] bytes = value.getByteArray();
        bytes[0] = ATypeTag.STRING.serialize();
        System.arraycopy(string.getByteArray(), string.getStartOffset(), bytes, 1, string.getLength());
        return value;
    }
}
