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
package org.apache.asterix.cloud.parquet;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.external.writer.printer.parquet.ISchemaChecker;
import org.apache.asterix.external.writer.printer.parquet.ParquetSchemaLazyVisitor;
import org.apache.asterix.external.writer.printer.parquet.ParquetSchemaTree;
import org.apache.hyracks.algebricks.runtime.writers.IExternalWriter;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class ParquetSchemaInferPoolWriter {
    private final ParquetExternalWriterFactory writerFactory;

    private List<ParquetSchemaTree.SchemaNode> schemaNodes;
    private List<IExternalWriter> writerList;
    private final int maxSchemas;
    private ISchemaChecker schemaChecker;
    private ParquetSchemaLazyVisitor schemaLazyVisitor;

    public ParquetSchemaInferPoolWriter(ParquetExternalWriterFactory writerFactory, ISchemaChecker schemaChecker,
            ParquetSchemaLazyVisitor parquetSchemaLazyVisitor, int maxSchemas) {
        this.writerFactory = writerFactory;
        this.schemaChecker = schemaChecker;
        this.schemaLazyVisitor = parquetSchemaLazyVisitor;
        this.maxSchemas = maxSchemas;
        this.schemaNodes = new ArrayList<>();
        this.writerList = new ArrayList<>();
    }

    public void inferSchema(IValueReference value) throws HyracksDataException {
        for (int i = 0; i < schemaNodes.size(); i++) {
            ISchemaChecker.SchemaComparisonType schemaComparisonType =
                    schemaChecker.checkSchema(schemaNodes.get(i), value);

            if (schemaComparisonType.equals(ISchemaChecker.SchemaComparisonType.EQUIVALENT)) {
                return;
            } else if (schemaComparisonType.equals(ISchemaChecker.SchemaComparisonType.GROWING)) {
                schemaNodes.set(i, schemaLazyVisitor.inferSchema(value));
                closeWriter(i);
                return;
            }
        }

        if (schemaNodes.size() == maxSchemas) {
            throw new HyracksDataException(ErrorCode.SCHEMA_LIMIT_EXCEEDED, maxSchemas);
        }
        schemaNodes.add(schemaLazyVisitor.inferSchema(value));
        writerList.add(null);
    }

    public void initNewPartition(IFrameTupleReference tuple) throws HyracksDataException {
        for (int i = 0; i < writerList.size(); i++) {
            createOrInitPartition(i, tuple);
        }
    }

    public void write(IValueReference value) throws HyracksDataException {
        for (int i = 0; i < schemaNodes.size(); i++) {
            if (schemaChecker.checkSchema(schemaNodes.get(i), value)
                    .equals(ISchemaChecker.SchemaComparisonType.EQUIVALENT)) {
                createOrWrite(i, value);
                return;
            }
        }
    }

    public void close() throws HyracksDataException {
        for (int i = 0; i < writerList.size(); i++) {
            closeWriter(i);
        }
    }

    private void createOrInitPartition(int index, IFrameTupleReference tupleReference) throws HyracksDataException {
        if (writerList.get(index) == null) {
            createWriter(index);
        }
        writerList.get(index).initNewPartition(tupleReference);
    }

    private void createOrWrite(int index, IValueReference value) throws HyracksDataException {
        if (writerList.get(index) == null) {
            createWriter(index);
        }
        writerList.get(index).write(value);
    }

    private void createWriter(int index) throws HyracksDataException {
        writerList.set(index, writerFactory.createWriter(schemaNodes.get(index)));
        writerList.get(index).open();
    }

    private void closeWriter(int index) throws HyracksDataException {
        if (writerList.get(index) != null) {
            writerList.get(index).close();
            writerList.set(index, null);
        }
    }

}
