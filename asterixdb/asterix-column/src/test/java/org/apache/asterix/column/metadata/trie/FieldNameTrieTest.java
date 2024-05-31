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

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.column.metadata.FieldNameTrie;
import org.apache.asterix.column.metadata.FieldNamesTrieDictionary;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleInputStream;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import org.apache.hyracks.data.std.util.RewindableDataOutputStream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FieldNameTrieTest {

    private static final int NOT_FOUND_INDEX = -1;
    private FieldNameTrie trie;
    private FieldNamesTrieDictionary dictionary;

    @Before
    public void init() {
        trie = new FieldNameTrie();
        dictionary = new FieldNamesTrieDictionary();
    }

    @After
    public void cleanup() {
        if (trie != null) {
            trie.clear();
        }
    }

    @Test
    public void asciiTest() throws HyracksDataException {
        List<String> fieldNames =
                List.of("ant", "age", "agent", "agitation", "agitated", "name", "aghast", "anti", "anon");

        assertInsertAndLookup(fieldNames);
    }

    @Test
    public void insertTest1() throws HyracksDataException {
        List<String> fieldNames = List.of("ap", "app", "apple");
        assertInsertAndLookup(fieldNames);
    }

    @Test
    public void insertTest2() throws HyracksDataException {
        List<String> fieldNames = List.of("ap", "apple", "app");
        assertInsertAndLookup(fieldNames);
    }

    @Test
    public void insertTest3() throws HyracksDataException {
        List<String> fieldNames = List.of("app", "apple", "ap");
        assertInsertAndLookup(fieldNames);
    }

    @Test
    public void insertTest4() throws HyracksDataException {
        List<String> fieldNames = List.of("app", "ap", "apple");
        assertInsertAndLookup(fieldNames);
    }

    @Test
    public void insertTest5() throws HyracksDataException {
        List<String> fieldNames = List.of("apple", "app", "ap");
        assertInsertAndLookup(fieldNames);
    }

    @Test
    public void insertTest6() throws HyracksDataException {
        List<String> fieldNames = List.of("apple", "ap", "app");
        assertInsertAndLookup(fieldNames);
    }

    @Test
    public void unicodeTest() throws HyracksDataException {
        List<String> fieldNames = List.of(
                // Basic Insert and Search
                "apple", "app", "application", "苹果", "应用", "सेब", "आवेदन", "りんご", "アプリケーション", "تفاحة", "تطبيق",

                // Insert and Search with Emojis
                "😀", "😃", "😄", "😅", "😂", "🍎", "🍏", "🍐", "🍊", "🍋",

                // Insert and Search with Accented Characters
                "éléphant", "école", "étudiant", "über", "groß", "straße",

                // Edge Cases
                "", "a", "字", "م",

                // Mixed Scripts
                "apple苹果", "app应用", "applicationआवेदन", "こんにちはworld", "مرحباworld",

                // Special Symbols and Punctuation
                "hello!", "hello?", "hello.", "hello,", "!@#$%^&*()_+-=[]{}|;':,.<>/?",

                // Long Strings
                "a".repeat(1000), "あ".repeat(500),

                // Palindromes
                "madam", "racecar", "あいいあ",

                // Anagrams
                "listen", "silent", "inlets", "学校", "校学");

        assertInsertAndLookup(fieldNames);
    }

    @Test
    public void insertingDuplicates() throws HyracksDataException {
        List<String> fieldNames =
                List.of("ant", "age", "agent", "agitation", "agitated", "name", "aghast", "anti", "anon");

        for (String fieldName : fieldNames) {
            trie.insert(UTF8StringPointable.generateUTF8Pointable(fieldName));
            dictionary.getOrCreateFieldNameIndex(UTF8StringPointable.generateUTF8Pointable(fieldName));
        }

        assertInsertAndLookup(fieldNames);
    }

    @Test
    public void insertingRandomTest() throws IOException {
        String fileName = "dict-words.txt";
        try {
            UnicodeFieldNameGenerator.genFields(1000, 10, 200, 100, 0.9F, fileName);

            // reading the fields and storing in the array.
            List<IValueReference> fields = new ArrayList<>();
            try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
                String fieldName;
                while ((fieldName = reader.readLine()) != null) {
                    fields.add(UTF8StringPointable.generateUTF8Pointable(fieldName));
                }
            }

            for (IValueReference fieldName : fields) {
                trie.insert(fieldName);
            }

            for (IValueReference fieldName : fields) {
                Assert.assertNotEquals(NOT_FOUND_INDEX, trie.lookup(fieldName));
            }

            // check serde of unicode
            ByteArrayAccessibleOutputStream baaos;
            RewindableDataOutputStream dos;
            baaos = new ByteArrayAccessibleOutputStream();
            dos = new RewindableDataOutputStream(baaos);
            trie.serialize(dos);

            DataInputStream inputStream =
                    new DataInputStream(new ByteArrayAccessibleInputStream(baaos.getByteArray(), 0, baaos.size()));
            FieldNameTrie newDictionary = FieldNameTrie.deserialize(inputStream);

            // looking up on the de-serialized dictionary
            for (IValueReference fieldName : fields) {
                Assert.assertNotEquals(NOT_FOUND_INDEX, newDictionary.lookup(fieldName));
            }
        } finally {
            // deleting the file
            File dictFile = new File(fileName);
            dictFile.delete();
        }
    }

    @Test
    public void serializeDeserializeTest() throws IOException {
        List<String> fieldNames =
                List.of("ant", "age", "agent", "agitation", "agitated", "name", "aghast", "anti", "anon");

        assertInsertAndLookup(fieldNames);
        int expectedIndex;

        ByteArrayAccessibleOutputStream baaos;
        RewindableDataOutputStream dos;
        baaos = new ByteArrayAccessibleOutputStream();
        dos = new RewindableDataOutputStream(baaos);
        trie.serialize(dos);

        DataInputStream inputStream =
                new DataInputStream(new ByteArrayAccessibleInputStream(baaos.getByteArray(), 0, baaos.size()));
        FieldNameTrie newDictionary = FieldNameTrie.deserialize(inputStream);

        // looking up on the de-serialized dictionary
        expectedIndex = 0;
        for (String fieldName : fieldNames) {
            Assert.assertEquals(expectedIndex,
                    newDictionary.lookup(UTF8StringPointable.generateUTF8Pointable(fieldName)));
            expectedIndex++;
        }
    }

    @Test
    public void nameAlreadyPartOfAnotherField() throws HyracksDataException {
        String prefix = "level_";
        List<String> fieldNames = new ArrayList<>();

        for (int i = 24; i >= 0; i--) {
            fieldNames.add(prefix + i);
        }

        assertInsertAndLookup(fieldNames);
    }

    @Test
    public void chaining() throws HyracksDataException {
        List<String> fieldNames = new ArrayList<>();

        String word = "";
        for (int i = 0; i < 500; i++) {
            word += "a";
            fieldNames.add(word);
        }

        assertInsertAndLookup(fieldNames);
    }

    @Test
    public void split() throws HyracksDataException {
        List<String> fieldNames = new ArrayList<>();

        String word = "";
        for (int i = 0; i < 500; i++) {
            word += "a";
            fieldNames.add(word);
        }

        Collections.reverse(fieldNames);
        assertInsertAndLookup(fieldNames);
    }

    private void assertInsertAndLookup(List<String> fieldNames) throws HyracksDataException {
        for (String fieldName : fieldNames) {
            Assert.assertEquals(dictionary.getOrCreateFieldNameIndex(fieldName),
                    trie.insert(UTF8StringPointable.generateUTF8Pointable(fieldName)));
        }

        int expectedIndex = 0;
        for (String fieldName : fieldNames) {
            IValueReference serializedFieldName = UTF8StringPointable.generateUTF8Pointable(fieldName);
            Assert.assertEquals(expectedIndex, trie.lookup(serializedFieldName));
            Assert.assertEquals(expectedIndex, dictionary.getFieldNameIndex(fieldName));
            Assert.assertEquals(expectedIndex, dictionary.getOrCreateFieldNameIndex(serializedFieldName));
            expectedIndex++;
        }
    }
}
