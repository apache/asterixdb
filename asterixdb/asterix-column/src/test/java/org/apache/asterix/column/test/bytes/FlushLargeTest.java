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
package org.apache.asterix.column.test.bytes;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.column.assembler.value.ValueGetterFactory;
import org.apache.asterix.column.common.buffer.DummyPage;
import org.apache.asterix.column.common.test.TestCase;
import org.apache.asterix.column.filter.NoOpColumnFilterEvaluatorFactory;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.column.operation.query.QueryColumnMetadata;
import org.apache.asterix.column.values.reader.ColumnValueReaderFactory;
import org.apache.asterix.common.exceptions.NoOpWarningCollector;
import org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.ColumnProjectorType;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class FlushLargeTest extends AbstractBytesTest {
    public FlushLargeTest(TestCase testCase) throws HyracksDataException {
        super(testCase);
    }

    @BeforeClass
    public static void setup() throws IOException {
        setup(FlushLargeTest.class);
    }

    @Parameters(name = "LargeTest {index}: {0}")
    public static Collection<Object[]> tests() throws Exception {
        return initTests(FlushLargeTest.class, "small");
    }

    @Test
    public void runLarge() throws IOException {
        int numberOfTuplesToWrite = 1000;
        int fileId = createFile();
        FlushColumnMetadata columnMetadata = prepareNewFile(fileId);
        List<IValueReference> record = getParsedRecords();
        List<DummyPage> pageZeros = transform(fileId, columnMetadata, record, numberOfTuplesToWrite);
        QueryColumnMetadata readMetadata = QueryColumnMetadata.create(columnMetadata.getDatasetType(),
                columnMetadata.getNumberOfPrimaryKeys(), columnMetadata.serializeColumnsMetadata(),
                new ColumnValueReaderFactory(), ValueGetterFactory.INSTANCE,
                ProjectionFiltrationTypeUtil.ALL_FIELDS_TYPE, Collections.emptyMap(),
                NoOpColumnFilterEvaluatorFactory.INSTANCE, NoOpColumnFilterEvaluatorFactory.INSTANCE,
                NoOpWarningCollector.INSTANCE, null, ColumnProjectorType.QUERY);
        writeResult(fileId, readMetadata, pageZeros);
        testCase.compareRepeated(numberOfTuplesToWrite);
    }
}
