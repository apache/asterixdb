/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.asterix.test.aql;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;

import junit.framework.Test;
import junit.framework.TestSuite;
import edu.uci.ics.asterix.aql.parser.ParseException;

public class AQLTestSuite extends TestSuite {
    private static String AQLTS_PATH = "src/test/resources/AQLTS/queries/";

    public static Test suite() throws ParseException, UnsupportedEncodingException, FileNotFoundException {
        File testData = new File(AQLTS_PATH);
        File[] queries = testData.listFiles();
        TestSuite testSuite = new TestSuite();
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
