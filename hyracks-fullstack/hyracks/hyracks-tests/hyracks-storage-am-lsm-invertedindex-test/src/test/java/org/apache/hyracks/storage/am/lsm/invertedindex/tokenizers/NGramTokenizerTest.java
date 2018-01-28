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

package org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hyracks.data.std.util.GrowableArray;
import org.apache.hyracks.util.string.UTF8StringReader;
import org.apache.hyracks.util.string.UTF8StringUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class NGramTokenizerTest {

    private char PRECHAR = '#';
    private char POSTCHAR = '$';

    private String str = "Jürgen S. Generic's Car";
    private byte[] inputBuffer;

    private int gramLength = 3;

    private void getExpectedGrams(String s, int gramLength, ArrayList<String> grams, boolean prePost) {

        String tmp = s.toLowerCase();
        if (prePost) {
            StringBuilder preBuilder = new StringBuilder();
            for (int i = 0; i < gramLength - 1; i++) {
                preBuilder.append(PRECHAR);
            }
            String pre = preBuilder.toString();

            StringBuilder postBuilder = new StringBuilder();
            for (int i = 0; i < gramLength - 1; i++) {
                postBuilder.append(POSTCHAR);
            }
            String post = postBuilder.toString();

            tmp = pre + s.toLowerCase() + post;
        }

        for (int i = 0; i < tmp.length() - gramLength + 1; i++) {
            String gram = tmp.substring(i, i + gramLength);
            grams.add(gram);
        }
    }

    @Before
    public void init() throws Exception {
        inputBuffer = UTF8StringUtil.writeStringToBytes(str);
    }

    void runTestNGramTokenizerWithCountedHashedUTF8Tokens(boolean prePost) throws IOException {
        HashedUTF8NGramTokenFactory tokenFactory = new HashedUTF8NGramTokenFactory();
        NGramUTF8StringBinaryTokenizer tokenizer =
                new NGramUTF8StringBinaryTokenizer(gramLength, prePost, false, false, tokenFactory);
        tokenizer.reset(inputBuffer, 0, inputBuffer.length);

        ArrayList<String> expectedGrams = new ArrayList<String>();
        getExpectedGrams(str, gramLength, expectedGrams, prePost);
        ArrayList<Integer> expectedHashedGrams = new ArrayList<Integer>();
        HashMap<String, Integer> gramCounts = new HashMap<String, Integer>();
        for (String s : expectedGrams) {
            Integer count = gramCounts.get(s);
            if (count == null) {
                count = 1;
                gramCounts.put(s, count);
            } else {
                count++;
            }

            int hash = tokenHash(s, count);
            expectedHashedGrams.add(hash);
        }

        int tokenCount = 0;

        while (tokenizer.hasNext()) {
            tokenizer.next();

            // serialize hashed token
            GrowableArray tokenData = new GrowableArray();

            IToken token = tokenizer.getToken();
            token.serializeToken(tokenData);

            // deserialize token
            ByteArrayInputStream bais = new ByteArrayInputStream(tokenData.getByteArray());
            DataInput in = new DataInputStream(bais);

            Integer hashedGram = in.readInt();

            // System.out.println(hashedGram);

            Assert.assertEquals(expectedHashedGrams.get(tokenCount), hashedGram);

            tokenCount++;
        }
        // System.out.println("---------");
    }

    void runTestNGramTokenizerWithHashedUTF8Tokens(boolean prePost) throws IOException {
        HashedUTF8NGramTokenFactory tokenFactory = new HashedUTF8NGramTokenFactory();
        NGramUTF8StringBinaryTokenizer tokenizer =
                new NGramUTF8StringBinaryTokenizer(gramLength, prePost, true, false, tokenFactory);
        tokenizer.reset(inputBuffer, 0, inputBuffer.length);

        ArrayList<String> expectedGrams = new ArrayList<String>();
        getExpectedGrams(str, gramLength, expectedGrams, prePost);
        ArrayList<Integer> expectedHashedGrams = new ArrayList<Integer>();
        for (String s : expectedGrams) {
            int hash = tokenHash(s, 1);
            expectedHashedGrams.add(hash);
        }

        int tokenCount = 0;

        while (tokenizer.hasNext()) {
            tokenizer.next();

            // serialize hashed token
            GrowableArray tokenData = new GrowableArray();

            IToken token = tokenizer.getToken();
            token.serializeToken(tokenData);

            // deserialize token
            ByteArrayInputStream bais = new ByteArrayInputStream(tokenData.getByteArray());
            DataInput in = new DataInputStream(bais);

            Integer hashedGram = in.readInt();

            // System.out.println(hashedGram);

            Assert.assertEquals(expectedHashedGrams.get(tokenCount), hashedGram);

            tokenCount++;
        }
        // System.out.println("---------");
    }

    void runTestNGramTokenizerWithUTF8Tokens(boolean prePost) throws IOException {
        UTF8NGramTokenFactory tokenFactory = new UTF8NGramTokenFactory();
        NGramUTF8StringBinaryTokenizer tokenizer =
                new NGramUTF8StringBinaryTokenizer(gramLength, prePost, true, false, tokenFactory);
        tokenizer.reset(inputBuffer, 0, inputBuffer.length);

        ArrayList<String> expectedGrams = new ArrayList<String>();
        getExpectedGrams(str, gramLength, expectedGrams, prePost);

        int tokenCount = 0;

        while (tokenizer.hasNext()) {
            tokenizer.next();

            // serialize hashed token
            GrowableArray tokenData = new GrowableArray();

            IToken token = tokenizer.getToken();
            token.serializeToken(tokenData);

            // deserialize token
            ByteArrayInputStream bais = new ByteArrayInputStream(tokenData.getByteArray());
            DataInput in = new DataInputStream(bais);

            UTF8StringReader reader = new UTF8StringReader();
            String strGram = reader.readUTF(in);

            // System.out.println("\"" + strGram + "\"");

            Assert.assertEquals(expectedGrams.get(tokenCount), strGram);

            tokenCount++;
        }
        // System.out.println("---------");
    }

    @Test
    public void testNGramTokenizerWithCountedHashedUTF8Tokens() throws Exception {
        runTestNGramTokenizerWithCountedHashedUTF8Tokens(false);
        runTestNGramTokenizerWithCountedHashedUTF8Tokens(true);
    }

    @Test
    public void testNGramTokenizerWithHashedUTF8Tokens() throws Exception {
        runTestNGramTokenizerWithHashedUTF8Tokens(false);
        runTestNGramTokenizerWithHashedUTF8Tokens(true);
    }

    @Test
    public void testNGramTokenizerWithUTF8Tokens() throws IOException {
        runTestNGramTokenizerWithUTF8Tokens(false);
        runTestNGramTokenizerWithUTF8Tokens(true);
    }

    public int tokenHash(String token, int tokenCount) {
        int h = AbstractUTF8Token.GOLDEN_RATIO_32;
        for (int i = 0; i < token.length(); i++) {
            h ^= token.charAt(i);
            h *= AbstractUTF8Token.GOLDEN_RATIO_32;
        }
        return h + tokenCount;
    }
}
