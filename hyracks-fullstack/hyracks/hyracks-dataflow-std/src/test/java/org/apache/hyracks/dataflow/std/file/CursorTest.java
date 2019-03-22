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
package org.apache.hyracks.dataflow.std.file;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

import org.junit.Assert;

public class CursorTest {

    // @Test commented out due to ASTERIXDB-1881
    public void test() {
        FileInputStream in = null;
        BufferedReader reader = null;
        try {
            in = new FileInputStream(
                    Paths.get(getClass().getResource("/data/beer.txt").toURI()).toAbsolutePath().toString());
            reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            // skip header
            final FieldCursorForDelimitedDataParser cursor = new FieldCursorForDelimitedDataParser(reader, ',', '"');
            // get number of fields from header (first record is header)
            cursor.nextRecord();
            int numOfFields = 0;
            int expectedNumberOfRecords = 7307;
            while (cursor.nextField()) {
                numOfFields++;
            }

            int recordNumber = 0;
            while (cursor.nextRecord()) {
                int fieldNumber = 0;
                while (cursor.nextField()) {
                    if (cursor.isDoubleQuoteIncludedInThisField) {
                        cursor.eliminateDoubleQuote(cursor.buffer, cursor.fStart, cursor.fEnd - cursor.fStart);
                        cursor.fEnd -= cursor.doubleQuoteCount;
                    }
                    fieldNumber++;
                }
                if ((fieldNumber > numOfFields) || (fieldNumber < numOfFields)) {
                    System.err.println("Test case failed. Expected number of fields in each record is " + numOfFields
                            + " and record number " + recordNumber + " was found to have " + fieldNumber);
                    Assert.assertTrue(false);
                }
                recordNumber++;
            }
            if (recordNumber != expectedNumberOfRecords) {
                System.err.println("Test case failed. Expected number of records is " + expectedNumberOfRecords
                        + " and records was found to be " + recordNumber);
                Assert.assertTrue(false);
            } else {
                System.err.println("TEST PASSED: " + recordNumber + " were lexed successfully.");
            }
        } catch (final Exception e) {
            e.printStackTrace();
            assert (false);
        } finally {
            try {
                reader.close();
            } catch (final IOException e) {
                e.printStackTrace();
            }
        }
    }
}
