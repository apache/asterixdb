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

public class WordTokenizerTest {

    private String text = "Hello World, I would like to inform you of the importance of Foo Bar. Yes, Foo Bar. Jürgen.";
    private byte[] inputBuffer;

    private ArrayList<String> expectedUTF8Tokens = new ArrayList<String>();
    private ArrayList<Integer> expectedHashedUTF8Tokens = new ArrayList<Integer>();
    private ArrayList<Integer> expectedCountedHashedUTF8Tokens = new ArrayList<Integer>();

    private boolean isSeparator(char c) {
        return !(Character.isLetterOrDigit(c) || Character.getType(c) == Character.OTHER_LETTER
                || Character.getType(c) == Character.OTHER_NUMBER);
    }

    private void tokenize(String text, ArrayList<String> tokens) {
        String lowerCaseText = text.toLowerCase();
        int startIx = 0;

        // Skip separators at beginning of string.
        while (isSeparator(lowerCaseText.charAt(startIx))) {
            startIx++;
        }
        while (startIx < lowerCaseText.length()) {
            while (startIx < lowerCaseText.length() && isSeparator(lowerCaseText.charAt(startIx))) {
                startIx++;
            }
            int tokenStart = startIx;

            while (startIx < lowerCaseText.length() && !isSeparator(lowerCaseText.charAt(startIx))) {
                startIx++;
            }
            int tokenEnd = startIx;

            // Emit token.
            String token = lowerCaseText.substring(tokenStart, tokenEnd);

            tokens.add(token);
        }
    }

    @Before
    public void init() throws IOException {
        // serialize text into bytes
        inputBuffer = UTF8StringUtil.writeStringToBytes(text);

        // init expected string tokens
        tokenize(text, expectedUTF8Tokens);

        // hashed tokens ignoring token count
        for (int i = 0; i < expectedUTF8Tokens.size(); i++) {
            int hash = tokenHash(expectedUTF8Tokens.get(i), 1);
            expectedHashedUTF8Tokens.add(hash);
        }

        // hashed tokens using token count
        HashMap<String, Integer> tokenCounts = new HashMap<String, Integer>();
        for (int i = 0; i < expectedUTF8Tokens.size(); i++) {
            Integer count = tokenCounts.get(expectedUTF8Tokens.get(i));
            if (count == null) {
                count = 1;
                tokenCounts.put(expectedUTF8Tokens.get(i), count);
            } else {
                count++;
            }

            int hash = tokenHash(expectedUTF8Tokens.get(i), count);
            expectedCountedHashedUTF8Tokens.add(hash);
        }
    }

    @Test
    public void testWordTokenizerWithCountedHashedUTF8Tokens() throws IOException {

        HashedUTF8WordTokenFactory tokenFactory = new HashedUTF8WordTokenFactory();
        DelimitedUTF8StringBinaryTokenizer tokenizer =
                new DelimitedUTF8StringBinaryTokenizer(false, false, tokenFactory);

        tokenizer.reset(inputBuffer, 0, inputBuffer.length);

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

            Integer hashedToken = in.readInt();

            Assert.assertEquals(hashedToken, expectedCountedHashedUTF8Tokens.get(tokenCount));

            tokenCount++;
        }
    }

    @Test
    public void testWordTokenizerWithHashedUTF8Tokens() throws IOException {

        HashedUTF8WordTokenFactory tokenFactory = new HashedUTF8WordTokenFactory();
        DelimitedUTF8StringBinaryTokenizer tokenizer =
                new DelimitedUTF8StringBinaryTokenizer(true, false, tokenFactory);

        tokenizer.reset(inputBuffer, 0, inputBuffer.length);

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

            Integer hashedToken = in.readInt();

            Assert.assertEquals(expectedHashedUTF8Tokens.get(tokenCount), hashedToken);

            tokenCount++;
        }
    }

    @Test
    public void testWordTokenizerWithUTF8Tokens() throws IOException {

        UTF8WordTokenFactory tokenFactory = new UTF8WordTokenFactory();
        DelimitedUTF8StringBinaryTokenizer tokenizer =
                new DelimitedUTF8StringBinaryTokenizer(true, false, tokenFactory);

        tokenizer.reset(inputBuffer, 0, inputBuffer.length);

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
            String strToken = reader.readUTF(in);

            Assert.assertEquals(expectedUTF8Tokens.get(tokenCount), strToken);

            tokenCount++;
        }
    }

    // JAQL Hash
    public int tokenHash(String token, int tokenCount) {
        int h = AbstractUTF8Token.GOLDEN_RATIO_32;
        for (int i = 0; i < token.length(); i++) {
            h ^= token.charAt(i);
            h *= AbstractUTF8Token.GOLDEN_RATIO_32;
        }
        return h + tokenCount;
    }
}
