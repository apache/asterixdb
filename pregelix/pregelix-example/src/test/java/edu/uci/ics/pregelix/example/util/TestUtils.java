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
package edu.uci.ics.pregelix.example.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.junit.Assert;

public class TestUtils {

    private static final String PREFIX = "part";

    public static void compareWithResultDir(File expectedFileDir, File actualFileDir) throws Exception {
        Collection<Record> expectedRecords = loadRecords(expectedFileDir);
        Collection<Record> actualRecords = loadRecords(actualFileDir);
        boolean equal = collectionEqual(expectedRecords, actualRecords);
        Assert.assertTrue(equal);
    }

    public static boolean collectionEqual(Collection<Record> c1, Collection<Record> c2) {
        for (Record r1 : c1) {
            boolean exists = false;
            for (Record r2 : c2) {
                if (r1.equals(r2)) {
                    exists = true;
                    break;
                }
            }
            if (!exists) {
                return false;
            }
        }
        for (Record r2 : c2) {
            boolean exists = false;
            for (Record r1 : c1) {
                if (r2.equals(r1)) {
                    exists = true;
                    break;
                }
            }
            if (!exists) {
                return false;
            }
        }
        return true;
    }

    public static void compareWithResult(File expectedFile, File actualFile) throws Exception {
        Collection<Record> expectedRecords = new ArrayList<Record>();
        Collection<Record> actualRecords = new ArrayList<Record>();
        populateResultFile(expectedRecords, expectedFile);
        populateResultFile(actualRecords, actualFile);
        boolean equal = expectedRecords.equals(actualRecords);
        Assert.assertTrue(equal);
    }

    private static Collection<Record> loadRecords(File dir) throws Exception {
        String[] fileNames = dir.list();
        Collection<Record> records = new ArrayList<Record>();
        for (String fileName : fileNames) {
            if (fileName.startsWith(PREFIX)) {
                File file = new File(dir, fileName);
                populateResultFile(records, file);
            }
        }
        return records;
    }

    private static void populateResultFile(Collection<Record> records, File file) throws FileNotFoundException,
            IOException {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line = null;
        while ((line = reader.readLine()) != null) {
            records.add(new Record(line));
        }
        reader.close();
    }

}
