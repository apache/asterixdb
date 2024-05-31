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
package org.apache.asterix.column.metadata;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public class FieldNamesTrieDictionary extends AbstractFieldNamesDictionary {
    private FieldNameTrie dictionary;
    //For lookups
    private final ArrayBackedValueStorage lookupStorage;

    public FieldNamesTrieDictionary() {
        this(new FieldNameTrie());
    }

    public FieldNamesTrieDictionary(FieldNameTrie dictionary) {
        super();
        this.dictionary = dictionary;
        lookupStorage = new ArrayBackedValueStorage();
    }

    @Override
    public List<IValueReference> getFieldNames() {
        return dictionary.getFieldNames();
    }

    @Override
    public int getOrCreateFieldNameIndex(IValueReference fieldName) throws HyracksDataException {
        if (fieldName == DUMMY_FIELD_NAME) {
            return DUMMY_FIELD_NAME_INDEX;
        }

        return dictionary.insert(fieldName);
    }

    @Override
    public int getOrCreateFieldNameIndex(String fieldName) throws HyracksDataException {
        return getOrCreateFieldNameIndex(creatFieldName(fieldName));
    }

    @Override
    public int getFieldNameIndex(String fieldName) throws HyracksDataException {
        lookupStorage.reset();
        serializeFieldName(fieldName, lookupStorage);
        return dictionary.lookup(lookupStorage);
    }

    @Override
    public IValueReference getFieldName(int index) {
        if (index == DUMMY_FIELD_NAME_INDEX) {
            return DUMMY_FIELD_NAME;
        }
        return dictionary.getFieldName(index);
    }

    @Override
    public void serialize(DataOutput output) throws IOException {
        dictionary.serialize(output);
    }

    public static FieldNamesTrieDictionary deserialize(DataInput input) throws IOException {
        FieldNameTrie fieldNameTrie = FieldNameTrie.deserialize(input);
        return new FieldNamesTrieDictionary(fieldNameTrie);
    }

    @Override
    public void abort(DataInputStream input) throws IOException {
        dictionary.clear();
        dictionary = FieldNameTrie.deserialize(input);
    }
}
