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
package org.apache.asterix.test.aql;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;

import org.apache.asterix.lang.aql.parser.ParseException;
import org.apache.commons.lang3.StringUtils;

import junit.framework.Test;
import junit.framework.TestSuite;

public class AQLTestSuite extends TestSuite {
    private static String AQLTS_PATH = StringUtils
            .join(new String[] { "src", "test", "resources", "parserts", "queries" + File.separator }, File.separator);
    private static String AQLTS_SQL_LIKE_PATH = StringUtils.join(
            new String[] { "src", "test", "resources", "parserts", "queries-sql-like" + File.separator },
            File.separator);

    public static Test suite() throws ParseException, UnsupportedEncodingException, FileNotFoundException {
        File testData = new File(AQLTS_PATH);
        File[] queries = testData.listFiles();
        TestSuite testSuite = new TestSuite();
        for (File file : queries) {
            if (file.isFile()) {
                testSuite.addTest(new AQLTestCase(file));
            }
        }
        testData = new File(AQLTS_SQL_LIKE_PATH);
        queries = testData.listFiles();
        for (File file : queries) {
            if (file.isFile()) {
                testSuite.addTest(new AQLTestCase(file));
            }
        }
        return testSuite;
    }

    public static void main(String args[]) throws Throwable {
        junit.textui.TestRunner.run(AQLTestSuite.suite());
    }
}
