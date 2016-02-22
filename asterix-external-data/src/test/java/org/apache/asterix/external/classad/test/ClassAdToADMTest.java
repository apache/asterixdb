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

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.classad.CaseInsensitiveString;
import org.apache.asterix.external.classad.CharArrayLexerSource;
import org.apache.asterix.external.classad.ClassAd;
import org.apache.asterix.external.classad.ExprTree;
import org.apache.asterix.external.classad.Value;
import org.apache.asterix.external.input.record.reader.stream.SemiStructuredRecordReader;
import org.apache.asterix.external.input.stream.LocalFileSystemInputStream;
import org.apache.asterix.external.library.ClassAdParser;
import org.apache.asterix.external.util.ExternalDataConstants;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class ClassAdToADMTest extends TestCase {
    /**
     * Create the test case
     *
     * @param testName
     *            name of the test case
     */
    public ClassAdToADMTest(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(ClassAdToADMTest.class);
    }

    /**
     *
     */
    public void testApp() {
        try {
            // test here
            ClassAd pAd = new ClassAd();
            String[] files = new String[] { "/jobads.txt" };
            ClassAdParser parser = new ClassAdParser();
            CharArrayLexerSource lexerSource = new CharArrayLexerSource();
            for (String path : files) {
                SemiStructuredRecordReader recordReader = new SemiStructuredRecordReader();
                HashMap<String, String> configuration = new HashMap<String, String>();
                configuration.put(ExternalDataConstants.KEY_RECORD_START, "[");
                configuration.put(ExternalDataConstants.KEY_RECORD_END, "]");
                recordReader.configure(configuration);
                LocalFileSystemInputStream in = new LocalFileSystemInputStream(
                        Paths.get(getClass().getResource(path).toURI()), null, false);
                in.configure(configuration);
                recordReader.setInputStream(in);
                Value val = new Value();
                int i = 0;
                while (recordReader.hasNext()) {
                    i++;
                    System.out.print("{ ");
                    val.clear();
                    IRawRecord<char[]> record = recordReader.next();
                    lexerSource.setNewSource(record.get());
                    parser.setLexerSource(lexerSource);
                    parser.parseNext(pAd);
                    //System.out.println(pAd);
                    Map<CaseInsensitiveString, ExprTree> attrs = pAd.getAttrList();
                    boolean notFirst = false;
                    for (Entry<CaseInsensitiveString, ExprTree> entry : attrs.entrySet()) {
                        CaseInsensitiveString name = entry.getKey();
                        ExprTree tree = entry.getValue();
                        if (notFirst) {
                            System.out.print(", ");
                        }
                        notFirst = true;
                        switch (tree.getKind()) {
                            case ATTRREF_NODE:
                            case CLASSAD_NODE:
                            case EXPR_ENVELOPE:
                            case EXPR_LIST_NODE:
                            case FN_CALL_NODE:
                            case OP_NODE:
                                if (pAd.evaluateAttr(name.get(), val)) {
                                    System.out.print("\"" + name + "Expr\":" + "\"expr=" + tree + "\"");
                                    System.out.print(", \"" + name + "\":" + val);
                                } else {
                                    System.out.print("\"" + name + "\":" + tree);
                                }
                                break;
                            case LITERAL_NODE:
                                // No need to do anything
                                System.out.print("\"" + name + "\":" + tree);
                                break;
                            default:
                                System.out.println("Something is wrong");
                                break;
                        }
                    }
                    System.out.println(" }");
                }
                System.out.println(i + " number of records found");
                recordReader.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }
}
