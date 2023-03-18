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

import java.io.IOException;
import java.util.Collections;

import org.apache.asterix.column.common.buffer.NoOpWriteMultiPageOp;
import org.apache.asterix.column.common.test.TestBase;
import org.apache.asterix.column.common.test.TestCase;
import org.apache.asterix.column.metadata.schema.ObjectSchemaNode;
import org.apache.asterix.column.operation.lsm.flush.BatchFinalizerVisitor;
import org.apache.asterix.column.operation.lsm.flush.ColumnTransformer;
import org.apache.asterix.column.operation.lsm.flush.FlushColumnMetadata;
import org.apache.asterix.column.values.writer.DummyColumnValuesWriterFactory;
import org.apache.asterix.column.values.writer.NoOpColumnBatchWriter;
import org.apache.asterix.om.lazy.RecordLazyVisitablePointable;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;

public abstract class AbstractDummyTest extends TestBase {
    private static final DummyColumnValuesWriterFactory WRITER_FACTORY = new DummyColumnValuesWriterFactory();
    protected final FlushColumnMetadata columnMetadata;
    protected final ColumnTransformer columnTransformer;
    protected final BatchFinalizerVisitor finalizer;
    //Schema
    protected final ArrayBackedValueStorage storage;
    protected final RecordLazyVisitablePointable pointable;
    protected int numberOfTuples;

    protected AbstractDummyTest(TestCase testCase) throws HyracksDataException {
        super(testCase);
        columnMetadata = new FlushColumnMetadata(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE, null,
                Collections.emptyList(), null, WRITER_FACTORY, new MutableObject<>(NoOpWriteMultiPageOp.INSTANCE));
        columnTransformer = new ColumnTransformer(columnMetadata, columnMetadata.getRoot());
        finalizer = new BatchFinalizerVisitor(columnMetadata);
        storage = new ArrayBackedValueStorage();
        pointable = new RecordLazyVisitablePointable(true);
    }

    public ObjectSchemaNode transform() throws IOException {
        storage.reset();
        while (parser.parse(storage.getDataOutput())) {
            pointable.set(storage);
            columnTransformer.transform(pointable);
            storage.reset();
            numberOfTuples++;
        }
        finalizer.finalizeBatch(NoOpColumnBatchWriter.INSTANCE, columnMetadata);
        return columnMetadata.getRoot();
    }
}
