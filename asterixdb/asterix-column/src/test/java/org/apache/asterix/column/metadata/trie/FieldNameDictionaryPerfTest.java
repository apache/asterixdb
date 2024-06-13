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
package org.apache.asterix.column.metadata.trie;

import java.util.concurrent.TimeUnit;

import org.apache.asterix.column.metadata.IFieldNamesDictionary;
import org.apache.asterix.column.metadata.dictionary.FieldNamesHashDictionary;
import org.apache.asterix.column.metadata.dictionary.FieldNamesTrieDictionary;
import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.om.base.AMutableString;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.util.string.UTF8StringReader;
import org.apache.hyracks.util.string.UTF8StringWriter;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class FieldNameDictionaryPerfTest {
    private static final int NUM_RECORDS = 1000000;
    private static final int NUMBER_OF_RANDOM_FIELD_NAMES = 1000;
    private static final int NUM_ITER = 5;
    private static final String[] FIELD_NAMES = { "country", "address", "free_parking", "city", "type", "url",
            "reviews", "date", "author", "ratings", "Value", "Cleanliness", "Overall", "Check in / front desk", "Rooms",
            "date", "author", "ratings", "Value", "Cleanliness", "Overall", "Check in / front desk", "Rooms", "date",
            "author", "ratings", "Value", "Cleanliness", "Overall", "Check in / front desk", "Rooms", "phone", "price",
            "avg_rating", "free_breakfast", "name", "public_likes", "email" };
    private static final FieldNameDictionaryFactory HASH = FieldNamesHashDictionary::new;
    private static final FieldNameDictionaryFactory TRIE = FieldNamesTrieDictionary::new;

    private final AStringSerializerDeserializer stringSerDer =
            new AStringSerializerDeserializer(new UTF8StringWriter(), new UTF8StringReader());
    private final AMutableString string = new AMutableString("");

    @Test
    public void benchmarkRandom() throws HyracksDataException {
        IValueReference[] fieldNames = new IValueReference[NUMBER_OF_RANDOM_FIELD_NAMES];
        for (int i = 0; i < NUMBER_OF_RANDOM_FIELD_NAMES; i++) {
            fieldNames[i] = getRandomString();
        }
        runAndReportTime(fieldNames);
    }

    @Test
    public void benchmarkRepeated() throws HyracksDataException {
        IValueReference[] fieldNames = new IValueReference[FIELD_NAMES.length];
        for (int i = 0; i < FIELD_NAMES.length; i++) {
            fieldNames[i] = serialize(FIELD_NAMES[i]);
        }

        runAndReportTime(fieldNames);
    }

    private void runAndReportTime(IValueReference[] fieldNames) throws HyracksDataException {
        long start;

        start = System.nanoTime();
        for (int i = 0; i < NUM_ITER; i++) {
            createAndRun(HASH, fieldNames);
        }
        System.out.println("HASH: " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));

        start = System.nanoTime();
        for (int i = 0; i < NUM_ITER; i++) {
            createAndRun(TRIE, fieldNames);
        }
        System.out.println("TRIE: " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
    }

    private void createAndRun(FieldNameDictionaryFactory factory, IValueReference[] fieldNames)
            throws HyracksDataException {
        IFieldNamesDictionary dictionary = factory.create();
        for (int i = 0; i < NUM_RECORDS; i++) {
            for (int j = 0; j < fieldNames.length; j++) {
                dictionary.getOrCreateFieldNameIndex(fieldNames[j]);
            }
        }
    }

    private IValueReference getRandomString() throws HyracksDataException {
        return serialize(RandomStringUtils.randomAlphanumeric(5, 20));
    }

    private IValueReference serialize(String value) throws HyracksDataException {
        ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
        storage.reset();
        string.setValue(value);
        stringSerDer.serialize(string, storage.getDataOutput());
        return storage;
    }

    @FunctionalInterface
    private interface FieldNameDictionaryFactory {
        IFieldNamesDictionary create();
    }
}
