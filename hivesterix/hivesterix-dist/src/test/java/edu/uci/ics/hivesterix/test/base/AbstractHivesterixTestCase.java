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
package edu.uci.ics.hivesterix.test.base;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.StringWriter;

import junit.framework.TestCase;

public class AbstractHivesterixTestCase extends TestCase {
    protected File queryFile;

    public AbstractHivesterixTestCase(String testName, File queryFile) {
        super(testName);
    }

    protected static void readFileToString(File file, StringBuilder buf) throws Exception {
        BufferedReader result = new BufferedReader(new FileReader(file));
        while (true) {
            String s = result.readLine();
            if (s == null) {
                break;
            } else {
                buf.append(s);
                buf.append('\n');
            }
        }
        result.close();
    }

    protected static void writeStringToFile(File file, StringWriter buf) throws Exception {
        PrintWriter result = new PrintWriter(new FileWriter(file));
        result.print(buf);
        result.close();
    }

    protected static void writeStringToFile(File file, StringBuilder buf) throws Exception {
        PrintWriter result = new PrintWriter(new FileWriter(file));
        result.print(buf);
        result.close();
    }

    protected static String removeExt(String fname) {
        int dot = fname.lastIndexOf('.');
        return fname.substring(0, dot);
    }
}
