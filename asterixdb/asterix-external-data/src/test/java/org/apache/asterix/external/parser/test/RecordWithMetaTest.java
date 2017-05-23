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
package org.apache.asterix.external.parser.test;

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
import org.apache.asterix.external.input.record.converter.CSVToRecordWithMetadataAndPKConverter;
import org.apache.asterix.external.input.record.reader.stream.LineRecordReader;
import org.apache.asterix.external.input.stream.LocalFSInputStream;
import org.apache.asterix.external.parser.ADMDataParser;
import org.apache.asterix.external.parser.RecordWithMetadataParser;
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

public class RecordWithMetaTest {
    private static ARecordType recordType;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    // @Test commented out due to ASTERIXDB-1881
    public void runTest() throws Exception {
        File file = new File("target/beer.adm");
        File expected = new File(getClass().getResource("/openbeerdb/beer.txt").toURI().getPath());
        try {
            FileUtils.deleteQuietly(file);
            PrintStream printStream = new PrintStream(Files.newOutputStream(Paths.get(file.toURI())));
            // create key type
            IAType[] keyTypes = { BuiltinType.ASTRING };
            String keyName = "id";
            List<String> keyNameAsList = new ArrayList<>(1);
            keyNameAsList.add(keyName);
            // create record type
            String[] recordFieldNames = {};
            IAType[] recordFieldTypes = {};
            recordType = new ARecordType("value", recordFieldNames, recordFieldTypes, true);
            // create the meta type
            String[] metaFieldNames = { keyName, "flags", "expiration", "cas", "rev", "vbid", "dtype" };
            IAType[] metaFieldTypes = { BuiltinType.ASTRING, BuiltinType.AINT32, BuiltinType.AINT64, BuiltinType.AINT64,
                    BuiltinType.AINT32, BuiltinType.AINT32, BuiltinType.AINT32 };
            ARecordType metaType = new ARecordType("meta", metaFieldNames, metaFieldTypes, true);
            int valueIndex = 4;
            char delimiter = ',';
            int numOfTupleFields = 3;
            int[] pkIndexes = { 0 };
            int[] pkIndicators = { 1 };

            List<Path> paths = new ArrayList<>();
            paths.add(Paths.get(getClass().getResource("/openbeerdb/beer.csv").toURI()));
            FileSystemWatcher watcher = new FileSystemWatcher(paths, null, false);
            // create input stream
            LocalFSInputStream inputStream = new LocalFSInputStream(watcher);
            // create reader record reader
            Map<String, String> config = new HashMap<>();
            config.put(ExternalDataConstants.KEY_HEADER, "true");
            config.put(ExternalDataConstants.KEY_QUOTE, ExternalDataConstants.DEFAULT_QUOTE);
            LineRecordReader lineReader = new LineRecordReader();
            lineReader.configure(inputStream, config);
            // create csv with json record reader
            CSVToRecordWithMetadataAndPKConverter recordConverter = new CSVToRecordWithMetadataAndPKConverter(
                    valueIndex, delimiter, metaType, recordType, pkIndicators, pkIndexes, keyTypes);
            // create the value parser <ADM in this case>
            ADMDataParser valueParser = new ADMDataParser(recordType, false);
            // create parser.
            RecordWithMetadataParser parser = new RecordWithMetadataParser(metaType, valueParser, recordConverter);

            // create serializer deserializer and printer factories
            ISerializerDeserializer[] serdes = new ISerializerDeserializer[keyTypes.length + 2];
            IPrinterFactory[] printerFactories = new IPrinterFactory[keyTypes.length + 2];
            for (int i = 0; i < keyTypes.length; i++) {
                serdes[i + 2] = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(keyTypes[i]);
                printerFactories[i + 2] = ADMPrinterFactoryProvider.INSTANCE.getPrinterFactory(keyTypes[i]);
            }
            serdes[0] = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(recordType);
            serdes[1] = SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(metaType);
            printerFactories[0] = ADMPrinterFactoryProvider.INSTANCE.getPrinterFactory(recordType);
            printerFactories[1] = ADMPrinterFactoryProvider.INSTANCE.getPrinterFactory(metaType);
            // create output descriptor
            IPrinter[] printers = new IPrinter[printerFactories.length];

            for (int i = 0; i < printerFactories.length; i++) {
                printers[i] = printerFactories[i].createPrinter();
            }

            ArrayTupleBuilder tb = new ArrayTupleBuilder(numOfTupleFields);
            while (lineReader.hasNext()) {
                IRawRecord<char[]> record = lineReader.next();
                tb.reset();
                parser.parse(record, tb.getDataOutput());
                tb.addFieldEndOffset();
                parser.parseMeta(tb.getDataOutput());
                tb.addFieldEndOffset();
                parser.appendLastParsedPrimaryKeyToTuple(tb);
                //print tuple
                printTuple(tb, printers, printStream);

            }
            lineReader.close();
            printStream.close();
            Assert.assertTrue(FileUtils.contentEquals(file, expected));
        } catch (Throwable th) {
            System.err.println("TEST FAILED");
            th.printStackTrace();
            throw th;
        } finally {
            FileUtils.deleteQuietly(file);
        }
        System.err.println("TEST PASSED.");
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
}
