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
package org.apache.asterix.external.classad.test;

import java.io.BufferedReader;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.asterix.external.classad.ClassAd;
import org.apache.asterix.external.classad.object.pool.ClassAdObjectPool;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class ClassAdParserTest extends TestCase {
    /**
     * Create the test case
     *
     * @param testName
     *            name of the test case
     */
    public ClassAdParserTest(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(ClassAdParserTest.class);
    }

    /**
     *
     */
    public void test() {
        try {
            // test here
            ClassAdObjectPool objectPool = new ClassAdObjectPool();
            ClassAd pAd = new ClassAd(objectPool);
            String szInput;
            String[] files = new String[] { "/classad/testdata.txt" };
            BufferedReader infile = null;
            for (String path : files) {
                infile = Files.newBufferedReader(
                        Paths.get(URLDecoder.decode(getClass().getResource(path).getPath(), "UTF-8")),
                        StandardCharsets.UTF_8);
                szInput = infile.readLine();
                while (szInput != null) {
                    if (szInput.trim().length() == 0) {
                        // ClassAdChain completed
                        pAd.clear();
                        objectPool.reset();
                    } else if (!pAd.insert(szInput)) {
                        // Problem
                        System.out.println("BARFED ON:" + szInput);
                        assert (false);
                    }
                    szInput = infile.readLine();
                }
                infile.close();
            }
        } catch (Exception e) {
            assertTrue(false);
        }
        assertTrue(true);
    }
}
