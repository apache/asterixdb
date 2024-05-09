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
package org.apache.asterix.column.test.dummy;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.column.assembler.value.DummyValueGetterFactory;
import org.apache.asterix.column.bytes.stream.in.AbstractBytesInputStream;
import org.apache.asterix.column.bytes.stream.in.DummyBytesInputStream;
import org.apache.asterix.column.common.test.TestCase;
import org.apache.asterix.column.filter.NoOpColumnFilterEvaluatorFactory;
import org.apache.asterix.column.operation.query.ColumnAssembler;
import org.apache.asterix.column.operation.query.QueryColumnMetadata;
import org.apache.asterix.column.util.RunLengthIntArray;
import org.apache.asterix.column.values.reader.DummyColumnValuesReaderFactory;
import org.apache.asterix.column.values.writer.DummyColumnValuesWriter;
import org.apache.asterix.common.exceptions.NoOpWarningCollector;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.printer.json.clean.APrintVisitor;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.ColumnProjectorType;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class AssemblerTest extends AbstractDummyTest {
    private final APrintVisitor printVisitor;
    private final ARecordVisitablePointable recordPointable;

    public AssemblerTest(TestCase testCase) throws HyracksDataException {
        super(testCase);
        printVisitor = new APrintVisitor();
        recordPointable = new ARecordVisitablePointable(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
    }

    /*
     * ***********************************************************************
     * Setup
     * ***********************************************************************
     */

    @BeforeClass
    public static void setup() throws IOException {
        setup(AssemblerTest.class);
    }

    @Parameters(name = "AssemblerTest {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        return initTests(AssemblerTest.class, "assembler");
    }

    /*
     * ***********************************************************************
     * Test
     * ***********************************************************************
     */

    @Test
    public void runTest() throws IOException, AlgebricksException {
        File testFile = testCase.getTestFile();
        prepareParser(testFile);
        transform();

        DummyColumnValuesReaderFactory readerFactory = createDummyColumnValuesReaderFactory();
        QueryColumnMetadata queryMetadata = QueryColumnMetadata.create(columnMetadata.getDatasetType(),
                columnMetadata.getNumberOfPrimaryKeys(), columnMetadata.serializeColumnsMetadata(), readerFactory,
                DummyValueGetterFactory.INSTANCE, ProjectionFiltrationTypeUtil.ALL_FIELDS_TYPE, Collections.emptyMap(),
                NoOpColumnFilterEvaluatorFactory.INSTANCE, NoOpColumnFilterEvaluatorFactory.INSTANCE,
                NoOpWarningCollector.INSTANCE, null, ColumnProjectorType.QUERY);
        AbstractBytesInputStream[] streams = new AbstractBytesInputStream[columnMetadata.getNumberOfColumns()];
        Arrays.fill(streams, DummyBytesInputStream.INSTANCE);

        writeResult(queryMetadata.getAssembler(), streams);
        testCase.compare();
    }

    private DummyColumnValuesReaderFactory createDummyColumnValuesReaderFactory() {
        List<RunLengthIntArray> defLevels = new ArrayList<>();
        List<List<IValueReference>> values = new ArrayList<>();
        for (int i = 0; i < columnMetadata.getNumberOfColumns(); i++) {
            DummyColumnValuesWriter writer = (DummyColumnValuesWriter) columnMetadata.getWriter(i);
            defLevels.add(writer.getDefinitionLevels());
            values.add(writer.getValues());
        }

        return new DummyColumnValuesReaderFactory(defLevels, values);
    }

    private void writeResult(ColumnAssembler assembler, AbstractBytesInputStream[] streams)
            throws FileNotFoundException, HyracksDataException {
        File resultFile = testCase.getOutputFile();
        try (PrintStream ps = new PrintStream(new FileOutputStream(resultFile))) {
            Pair<PrintStream, ATypeTag> pair = new Pair<>(ps, ATypeTag.OBJECT);
            assembler.reset(numberOfTuples);
            for (int i = 0; i < columnMetadata.getNumberOfColumns(); i++) {
                assembler.resetColumn(streams[i], i);
            }
            while (assembler.hasNext()) {
                IValueReference record = assembler.nextValue();
                recordPointable.set(record);
                recordPointable.accept(printVisitor, pair);
                ps.println();
            }
        }
    }

}
