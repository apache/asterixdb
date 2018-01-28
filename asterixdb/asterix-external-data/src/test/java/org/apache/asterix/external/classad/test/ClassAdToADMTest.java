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

import java.io.File;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.classad.CaseInsensitiveString;
import org.apache.asterix.external.classad.CharArrayLexerSource;
import org.apache.asterix.external.classad.ClassAd;
import org.apache.asterix.external.classad.ExprTree;
import org.apache.asterix.external.classad.Value;
import org.apache.asterix.external.classad.object.pool.ClassAdObjectPool;
import org.apache.asterix.external.input.record.reader.stream.SemiStructuredRecordReader;
import org.apache.asterix.external.input.stream.LocalFSInputStream;
import org.apache.asterix.external.library.ClassAdParser;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.FileSystemWatcher;
import org.apache.asterix.formats.nontagged.ADMPrinterFactoryProvider;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.commons.io.FileUtils;
import org.apache.hyracks.algebricks.data.IPrinter;
import org.apache.hyracks.algebricks.data.IPrinterFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.junit.Assert;

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

    private void printTuple(ArrayTupleBuilder tb, IPrinter[] printers, PrintStream printStream)
            throws HyracksDataException {
        int[] offsets = tb.getFieldEndOffsets();
        for (int i = 0; i < printers.length; i++) {
            int offset = i == 0 ? 0 : offsets[i - 1];
            int length = i == 0 ? offsets[0] : offsets[i] - offsets[i - 1];
            printers[i].print(tb.getByteArray(), offset, length, printStream);
            printStream.println();
        }
    }

    @SuppressWarnings("rawtypes")
    public void testSchemaful() {
        try {
            File file = new File("target/classad-wtih-temporals.adm");
            File expected =
                    new File(getClass().getResource("/classad/results/classad-with-temporals.adm").toURI().getPath());
            FileUtils.deleteQuietly(file);
            PrintStream printStream = new PrintStream(Files.newOutputStream(Paths.get(file.toURI())));
            String[] recordFieldNames = { "GlobalJobId", "Owner", "ClusterId", "ProcId", "RemoteWallClockTime",
                    "CompletionDate", "QDate", "JobCurrentStartDate", "JobStartDate", "JobCurrentStartExecutingDate" };
            IAType[] recordFieldTypes = { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.AINT32,
                    BuiltinType.AINT32, BuiltinType.ADURATION, BuiltinType.ADATETIME, BuiltinType.ADATETIME,
                    BuiltinType.ADATETIME, BuiltinType.ADATETIME, BuiltinType.ADATETIME };
            ARecordType recordType = new ARecordType("value", recordFieldNames, recordFieldTypes, true);
            int numOfTupleFields = 1;
            ISerializerDeserializer[] serdes = new ISerializerDeserializer[1];
            serdes[0] = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(recordType);
            IPrinterFactory[] printerFactories = new IPrinterFactory[1];
            printerFactories[0] = ADMPrinterFactoryProvider.INSTANCE.getPrinterFactory(recordType);
            // create output descriptor
            IPrinter[] printers = new IPrinter[printerFactories.length];
            for (int i = 0; i < printerFactories.length; i++) {
                printers[i] = printerFactories[i].createPrinter();
            }
            ClassAdObjectPool objectPool = new ClassAdObjectPool();
            String[] files = new String[] { "/classad/classad-with-temporals.classads" };
            ClassAdParser parser = new ClassAdParser(recordType, false, false, false, null, null, null, objectPool);
            ArrayTupleBuilder tb = new ArrayTupleBuilder(numOfTupleFields);
            for (String path : files) {
                List<Path> paths = new ArrayList<>();
                Map<String, String> config = new HashMap<>();
                config.put(ExternalDataConstants.KEY_RECORD_START, "[");
                config.put(ExternalDataConstants.KEY_RECORD_END, "]");
                paths.add(Paths.get(getClass().getResource(path).toURI()));
                FileSystemWatcher watcher = new FileSystemWatcher(paths, null, false);
                LocalFSInputStream in = new LocalFSInputStream(watcher);
                SemiStructuredRecordReader recordReader = new SemiStructuredRecordReader();
                recordReader.configure(in, config);
                while (recordReader.hasNext()) {
                    tb.reset();
                    IRawRecord<char[]> record = recordReader.next();
                    parser.parse(record, tb.getDataOutput());
                    tb.addFieldEndOffset();
                    printTuple(tb, printers, printStream);
                }
                recordReader.close();
                printStream.close();
                Assert.assertTrue(FileUtils.contentEquals(file, expected));
            }
        } catch (Throwable th) {
            System.err.println("TEST FAILED");
            th.printStackTrace();
            Assert.assertTrue(false);
        }
        System.err.println("TEST PASSED");
    }

    /**
    *
    */
    public void testEscaping() {
        try {
            ClassAdObjectPool objectPool = new ClassAdObjectPool();
            ClassAd pAd = new ClassAd(objectPool);
            String[] files = new String[] { "/classad/escapes.txt" };
            ClassAdParser parser = new ClassAdParser(objectPool);
            CharArrayLexerSource lexerSource = new CharArrayLexerSource();
            for (String path : files) {
                List<Path> paths = new ArrayList<>();
                Map<String, String> config = new HashMap<>();
                config.put(ExternalDataConstants.KEY_RECORD_START, "[");
                config.put(ExternalDataConstants.KEY_RECORD_END, "]");
                paths.add(Paths.get(getClass().getResource(path).toURI()));
                FileSystemWatcher watcher = new FileSystemWatcher(paths, null, false);
                LocalFSInputStream in = new LocalFSInputStream(watcher);
                SemiStructuredRecordReader recordReader = new SemiStructuredRecordReader();
                recordReader.configure(in, config);
                try {
                    Value val = new Value(objectPool);
                    while (recordReader.hasNext()) {
                        val.reset();
                        IRawRecord<char[]> record = recordReader.next();
                        lexerSource.setNewSource(record.get());
                        parser.setLexerSource(lexerSource);
                        parser.parseNext(pAd);
                        Assert.assertEquals(
                                "[ Args = \"“-1 0.1 0.1 0.5 2e-07 0.001 10 -1”\"; GlobalJobId = \"submit-4.chtc.wisc.edu#3724038.0#1462893042\" ]",
                                pAd.toString());
                    }
                } finally {
                    recordReader.close();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }

    /**
     *
     */
    public void testSchemaless() {
        try {
            ClassAdObjectPool objectPool = new ClassAdObjectPool();
            ClassAd pAd = new ClassAd(objectPool);
            String[] files = new String[] { "/classad/jobads.txt" };
            ClassAdParser parser = new ClassAdParser(objectPool);
            CharArrayLexerSource lexerSource = new CharArrayLexerSource();
            for (String path : files) {
                List<Path> paths = new ArrayList<>();
                Map<String, String> config = new HashMap<>();
                config.put(ExternalDataConstants.KEY_RECORD_START, "[");
                config.put(ExternalDataConstants.KEY_RECORD_END, "]");
                paths.add(Paths.get(getClass().getResource(path).toURI()));
                FileSystemWatcher watcher = new FileSystemWatcher(paths, null, false);
                LocalFSInputStream in = new LocalFSInputStream(watcher);
                SemiStructuredRecordReader recordReader = new SemiStructuredRecordReader();
                recordReader.configure(in, config);
                try {
                    Value val = new Value(objectPool);
                    while (recordReader.hasNext()) {
                        val.reset();
                        IRawRecord<char[]> record = recordReader.next();
                        lexerSource.setNewSource(record.get());
                        parser.setLexerSource(lexerSource);
                        parser.parseNext(pAd);
                        Map<CaseInsensitiveString, ExprTree> attrs = pAd.getAttrList();
                        attrs.forEach((key, tree) -> {
                            switch (tree.getKind()) {
                                case ATTRREF_NODE:
                                case CLASSAD_NODE:
                                case EXPR_ENVELOPE:
                                case EXPR_LIST_NODE:
                                case FN_CALL_NODE:
                                case OP_NODE:
                                    break;
                                case LITERAL_NODE:
                                    break;
                                default:
                                    System.out.println("Something is wrong");
                                    break;
                            }
                        });
                    }
                } finally {
                    recordReader.close();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }
}
