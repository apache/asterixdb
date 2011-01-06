/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ByteArrayAccessibleOutputStream;
import edu.uci.ics.hyracks.dataflow.common.data.hash.UTF8StringBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;

public class TokenizerTest {

    // testing DelimitedUTF8StringBinaryTokenizer
    @Test
    public void test01() throws Exception {
        Random rnd = new Random(50);

        int numDocs = 100;
        int maxWords = 1000;
        int maxWordLength = 50;
        char delimiter = ' ';

        DelimitedUTF8StringBinaryTokenizer tok = new DelimitedUTF8StringBinaryTokenizer(delimiter);

        // create a bunch of documents
        for (int i = 0; i < numDocs; i++) {

            // create a single document with a bunch of words
            int words = (Math.abs(rnd.nextInt()) % maxWords) + 1;
            StringBuilder strBuilder = new StringBuilder();
            for (int j = 0; j < words; j++) {
                int len = (Math.abs(rnd.nextInt()) % maxWordLength) + 1;
                String s = randomString(len, rnd);
                strBuilder.append(s);
                if (j < words - 1)
                    strBuilder.append(delimiter);
            }

            String doc = strBuilder.toString();

            // serialize document into baaos
            ByteArrayAccessibleOutputStream baaos = new ByteArrayAccessibleOutputStream();
            DataOutputStream dos = new DataOutputStream(baaos);
            UTF8StringSerializerDeserializer.INSTANCE.serialize(doc, dos);
            byte[] data = baaos.toByteArray();

            // use binary tokenizer and compare with Java tokenizer
            String[] cmpTokens = doc.split(new String(new char[] { delimiter }));
            int cmpCounter = 0;

            tok.reset(data, 0, data.length);
            while (tok.hasNext()) {
                tok.next();

                // write token to outputstream
                ByteArrayAccessibleOutputStream baaosWrite = new ByteArrayAccessibleOutputStream();
                DataOutputStream dosWrite = new DataOutputStream(baaosWrite);
                tok.writeToken(dosWrite);

                // deserialize token to get string object
                ByteArrayInputStream inStream = new ByteArrayInputStream(baaosWrite.toByteArray());
                DataInput dataIn = new DataInputStream(inStream);
                String s = UTF8StringSerializerDeserializer.INSTANCE.deserialize(dataIn);

                Assert.assertEquals(s, cmpTokens[cmpCounter++]);
            }
        }
    }

    // testing HashedQGramUTF8StringBinaryTokenizer
    @Test
    public void test02() throws Exception {
        Random rnd = new Random(50);

        int numStrings = 1000;
        int maxStrLen = 100;
        int minQ = 2;
        int maxQ = 10;

        // we test the correctness of HashedQGramUTF8StringBinaryTokenizer as
        // follows:
        // 1.1. tokenize the string into q-gram strings
        // 1.2. serialize q-gram strings into bytes
        // 1.3. compute hashed gram with UTF8StringBinaryHashFunctionFactory
        // 2.1. serialize string into bytes
        // 2.2. tokenize serialized string into hashed q-grams
        // 2.3. test whether hashed grams from 1.3. and 2.3. are equal
        for (int i = 0; i < numStrings; i++) {
            int q = (Math.abs(rnd.nextInt()) % (maxQ - minQ)) + minQ;
            int strLen = (Math.abs(rnd.nextInt()) % (maxStrLen - q)) + q;
            String str = randomString(strLen, rnd);

            // randomly choose pre and postfixing
            boolean prePost = false;
            if (Math.abs(rnd.nextInt()) % 2 == 0)
                prePost = true;

            HashedQGramUTF8StringBinaryTokenizer qgramTok = new HashedQGramUTF8StringBinaryTokenizer(q, prePost);

            String extendedString = str;
            if (prePost) {
                // pre and postfix string
                StringBuilder strBuilder = new StringBuilder();
                for (int j = 0; j < q - 1; j++)
                    strBuilder.append(qgramTok.getPreChar());
                strBuilder.append(str);
                for (int j = 0; j < q - 1; j++)
                    strBuilder.append(qgramTok.getPostChar());
                extendedString = strBuilder.toString();
            }

            // generate q-grams in deserialized form
            ArrayList<String> javaGrams = new ArrayList<String>();
            for (int j = 0; j < extendedString.length() - q + 1; j++) {
                javaGrams.add(extendedString.substring(j, j + q));
            }

            // serialize string for use in binary gram tokenizer
            ByteArrayAccessibleOutputStream baaos = new ByteArrayAccessibleOutputStream();
            DataOutputStream dos = new DataOutputStream(baaos);
            UTF8StringSerializerDeserializer.INSTANCE.serialize(str, dos);
            byte[] data = baaos.toByteArray();

            qgramTok.reset(data, 0, data.length);

            int counter = 0;
            while (qgramTok.hasNext()) {
                qgramTok.next();

                // write token to outputstream
                ByteArrayAccessibleOutputStream baaosWrite = new ByteArrayAccessibleOutputStream();
                DataOutputStream dosWrite = new DataOutputStream(baaosWrite);
                qgramTok.writeToken(dosWrite);

                // deserialize token to get hashed gram
                ByteArrayInputStream inStream = new ByteArrayInputStream(baaosWrite.toByteArray());
                DataInput dataIn = new DataInputStream(inStream);
                Integer binHashedGram = IntegerSerializerDeserializer.INSTANCE.deserialize(dataIn);

                // create hashed gram to test against
                ByteArrayAccessibleOutputStream baaosCmp = new ByteArrayAccessibleOutputStream();
                DataOutputStream dosCmp = new DataOutputStream(baaosCmp);
                UTF8StringSerializerDeserializer.INSTANCE.serialize(javaGrams.get(counter), dosCmp);

                IBinaryHashFunction strHasher = UTF8StringBinaryHashFunctionFactory.INSTANCE.createBinaryHashFunction();
                byte[] cmpData = baaosCmp.toByteArray();
                int cmpHash = strHasher.hash(cmpData, 0, cmpData.length);

                Assert.assertEquals(binHashedGram.intValue(), cmpHash);

                counter++;
            }
        }
    }

    public static String randomString(int length, Random random) {
        int maxAttempts = 1000;
        int count = 0;
        while (count < maxAttempts) {
            String s = Long.toHexString(Double.doubleToLongBits(random.nextDouble()));
            StringBuilder strBuilder = new StringBuilder();
            for (int i = 0; i < s.length() && i < length; i++) {
                strBuilder.append(s.charAt(Math.abs(random.nextInt()) % s.length()));
            }
            if (strBuilder.length() > 0)
                return strBuilder.toString();
            count++;
        }
        return "abc";
    }
}
