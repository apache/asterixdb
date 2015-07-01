/**
 * Copyright 2010-2011 The Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on
 * an "AS IS"; BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * Author: Alexander Behm <abehm (at) ics.uci.edu>
 */

package edu.uci.ics.asterix.fuzzyjoin.tests;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.asterix.fuzzyjoin.tokenizer.AbstractUTF8Token;
import edu.uci.ics.asterix.fuzzyjoin.tokenizer.DelimitedUTF8StringBinaryTokenizer;
import edu.uci.ics.asterix.fuzzyjoin.tokenizer.HashedUTF8WordTokenFactory;
import edu.uci.ics.asterix.fuzzyjoin.tokenizer.IToken;
import edu.uci.ics.asterix.fuzzyjoin.tokenizer.UTF8WordTokenFactory;

public class WordTokenizerTest {

    private String text = "Hello World, I would like to inform you of the importance of Foo Bar. Yes, Foo Bar. Jürgen.";
    private byte[] inputBuffer;

    private ArrayList<String> expectedUTF8Tokens = new ArrayList<String>();
    private ArrayList<Integer> expectedHashedUTF8Tokens = new ArrayList<Integer>();
    private ArrayList<Integer> expectedCountedHashedUTF8Tokens = new ArrayList<Integer>();

    @Before
    public void init() throws IOException {
        // serialize text into bytes
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput dos = new DataOutputStream(baos);
        dos.writeUTF(text);
        inputBuffer = baos.toByteArray();

        // init expected string tokens
        expectedUTF8Tokens.add("hello");
        expectedUTF8Tokens.add("world");
        expectedUTF8Tokens.add("i");
        expectedUTF8Tokens.add("would");
        expectedUTF8Tokens.add("like");
        expectedUTF8Tokens.add("to");
        expectedUTF8Tokens.add("inform");
        expectedUTF8Tokens.add("you");
        expectedUTF8Tokens.add("of");
        expectedUTF8Tokens.add("the");
        expectedUTF8Tokens.add("importance");
        expectedUTF8Tokens.add("of");
        expectedUTF8Tokens.add("foo");
        expectedUTF8Tokens.add("bar");
        expectedUTF8Tokens.add("yes");
        expectedUTF8Tokens.add("foo");
        expectedUTF8Tokens.add("bar");
        expectedUTF8Tokens.add("jürgen");

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
        DelimitedUTF8StringBinaryTokenizer tokenizer = new DelimitedUTF8StringBinaryTokenizer(false, false,
                tokenFactory);

        tokenizer.reset(inputBuffer, 0, inputBuffer.length);

        int tokenCount = 0;

        while (tokenizer.hasNext()) {
            tokenizer.next();

            // serialize token
            ByteArrayOutputStream tokenBaos = new ByteArrayOutputStream();
            DataOutput tokenDos = new DataOutputStream(tokenBaos);

            IToken token = tokenizer.getToken();
            token.serializeToken(tokenDos);

            // deserialize token
            ByteArrayInputStream bais = new ByteArrayInputStream(tokenBaos.toByteArray());
            DataInput in = new DataInputStream(bais);

            Integer hashedToken = in.readInt();

            // System.out.println(hashedToken);

            Assert.assertEquals(hashedToken, expectedCountedHashedUTF8Tokens.get(tokenCount));

            tokenCount++;
        }
    }

    @Test
    public void testWordTokenizerWithHashedUTF8Tokens() throws IOException {

        HashedUTF8WordTokenFactory tokenFactory = new HashedUTF8WordTokenFactory();
        DelimitedUTF8StringBinaryTokenizer tokenizer = new DelimitedUTF8StringBinaryTokenizer(true, false, tokenFactory);

        tokenizer.reset(inputBuffer, 0, inputBuffer.length);

        int tokenCount = 0;

        while (tokenizer.hasNext()) {
            tokenizer.next();

            // serialize token
            ByteArrayOutputStream tokenBaos = new ByteArrayOutputStream();
            DataOutput tokenDos = new DataOutputStream(tokenBaos);

            IToken token = tokenizer.getToken();
            token.serializeToken(tokenDos);

            // deserialize token
            ByteArrayInputStream bais = new ByteArrayInputStream(tokenBaos.toByteArray());
            DataInput in = new DataInputStream(bais);

            Integer hashedToken = in.readInt();

            // System.out.println(hashedToken);

            Assert.assertEquals(expectedHashedUTF8Tokens.get(tokenCount), hashedToken);

            tokenCount++;
        }
    }

    @Test
    public void testWordTokenizerWithUTF8Tokens() throws IOException {

        UTF8WordTokenFactory tokenFactory = new UTF8WordTokenFactory();
        DelimitedUTF8StringBinaryTokenizer tokenizer = new DelimitedUTF8StringBinaryTokenizer(true, false, tokenFactory);

        tokenizer.reset(inputBuffer, 0, inputBuffer.length);

        int tokenCount = 0;

        while (tokenizer.hasNext()) {
            tokenizer.next();

            // serialize hashed token
            ByteArrayOutputStream tokenBaos = new ByteArrayOutputStream();
            DataOutput tokenDos = new DataOutputStream(tokenBaos);

            IToken token = tokenizer.getToken();
            token.serializeToken(tokenDos);

            // deserialize token
            ByteArrayInputStream bais = new ByteArrayInputStream(tokenBaos.toByteArray());
            DataInput in = new DataInputStream(bais);

            String strToken = in.readUTF();

            // System.out.println(strToken);

            Assert.assertEquals(expectedUTF8Tokens.get(tokenCount), strToken);

            tokenCount++;
        }
    }

    // JAQL
    public int tokenHash(String token, int tokenCount) {
        int h = AbstractUTF8Token.GOLDEN_RATIO_32;
        for (int i = 0; i < token.length(); i++) {
            h ^= token.charAt(i);
            h *= AbstractUTF8Token.GOLDEN_RATIO_32;
        }
        return h + tokenCount;
    }
}
