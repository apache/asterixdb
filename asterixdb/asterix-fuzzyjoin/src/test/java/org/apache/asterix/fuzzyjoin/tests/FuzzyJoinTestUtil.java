/**
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

package org.apache.asterix.fuzzyjoin.tests;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;

import org.junit.Assert;

public class FuzzyJoinTestUtil {

    public static void verifyDirectory(String pathTest, String pathCorrect) throws IOException {
        verifyDirectory(pathTest, pathCorrect, false);
    }

    public static void verifyDirectory(String pathTest, String pathCorrect, boolean noDup) throws IOException {
        int countTestDedup = 0, countCorrect = 0;
        BufferedReader input;
        String line;
        HashSet<String> buffer = new HashSet<String>();

        // buffer Test
        input = new BufferedReader(new FileReader(pathTest));
        while ((line = input.readLine()) != null) {
            buffer.add(line);
        }
        countTestDedup = buffer.size();

        // probe Correct
        input = new BufferedReader(new FileReader(new File(pathCorrect)));
        while ((line = input.readLine()) != null) {
            Assert.assertTrue(buffer.contains(line));
            countCorrect++;
        }

        // check counts
        Assert.assertEquals(countTestDedup, countCorrect);
    }
}
