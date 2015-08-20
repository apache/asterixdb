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
package edu.uci.ics.algebricks.examples.piglet.test;

import java.io.File;

import junit.framework.Test;
import junit.framework.TestSuite;

public class PigletTest {
    public static Test suite() {
        TestSuite suite = new TestSuite();
        File dir = new File("testcases");
        findAndAddTests(suite, dir);

        return suite;
    }

    private static void findAndAddTests(TestSuite suite, File dir) {
        for (final File f : dir.listFiles()) {
            if (f.getName().startsWith(".")) {
                continue;
            }
            if (f.isDirectory()) {
                findAndAddTests(suite, f);
            } else if (f.getName().endsWith(".piglet")) {
                suite.addTest(new PigletTestCase(f));
            }
        }
    }
}
