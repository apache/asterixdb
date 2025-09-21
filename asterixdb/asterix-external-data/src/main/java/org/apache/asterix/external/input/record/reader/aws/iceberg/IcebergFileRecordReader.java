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
package org.apache.asterix.external.input.record.reader.aws.iceberg;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.dataflow.AbstractFeedDataFlowController;
import org.apache.asterix.external.input.record.GenericRecord;
import org.apache.asterix.external.util.IFeedLogManager;
import org.apache.asterix.external.util.iceberg.IcebergConstants;
import org.apache.asterix.external.util.iceberg.IcebergUtils;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;

/**
 * Iceberg record reader.
 * The reader returns records in Iceberg Record format.
 */
public class IcebergFileRecordReader implements IRecordReader<Record> {

    private final List<FileScanTask> fileScanTasks;
    private final Schema projectedSchema;
    private final Map<String, String> configuration;
    private final IRawRecord<Record> record;

    private int nextTaskIndex = 0;
    private Catalog catalog;
    private FileIO tableFileIo;
    private CloseableIterable<Record> iterable;
    private Iterator<Record> recordsIterator;

    public IcebergFileRecordReader(List<FileScanTask> fileScanTasks, Schema projectedSchema,
            Map<String, String> configuration) throws HyracksDataException {
        this.fileScanTasks = fileScanTasks;
        this.projectedSchema = projectedSchema;
        this.configuration = configuration;
        this.record = new GenericRecord<>();

        try {
            initializeTable();
        } catch (CompilationException e) {
            throw HyracksDataException.create(e);
        }
    }

    private void initializeTable() throws CompilationException {
        if (fileScanTasks.isEmpty()) {
            return;
        }

        String namespace = IcebergUtils.getNamespace(configuration);
        String tableName = configuration.get(IcebergConstants.ICEBERG_TABLE_NAME_PROPERTY_KEY);
        catalog = IcebergUtils.initializeCatalog(IcebergUtils.filterCatalogProperties(configuration), namespace);
        TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of(namespace), tableName);
        if (!catalog.tableExists(tableIdentifier)) {
            throw CompilationException.create(ErrorCode.ICEBERG_TABLE_DOES_NOT_EXIST, tableName);
        }
        Table table = catalog.loadTable(tableIdentifier);
        tableFileIo = table.io();
    }

    public boolean hasNext() throws Exception {
        // iterator has more records
        if (recordsIterator != null && recordsIterator.hasNext()) {
            return true;
        }

        // go to next task
        // if a file is empty, we will go to the next task
        while (nextTaskIndex < fileScanTasks.size()) {

            // close previous iterable
            if (iterable != null) {
                iterable.close();
                iterable = null;
            }

            // Load next task
            setNextRecordsIterator();

            // if the new iterator has rows → good
            if (recordsIterator != null && recordsIterator.hasNext()) {
                return true;
            }

            // else: this task is empty → continue the loop to the next task
        }

        // no more tasks & no more rows
        return false;
    }

    @Override
    public IRawRecord<Record> next() throws IOException, InterruptedException {
        Record icebergRecord = recordsIterator.next();
        record.set(icebergRecord);
        return record;
    }

    @Override
    public boolean stop() {
        return false;
    }

    @Override
    public void close() throws IOException {
        if (tableFileIo != null) {
            tableFileIo.close();
        }
        if (iterable != null) {
            iterable.close();
        }

        try {
            IcebergUtils.closeCatalog(catalog);
        } catch (CompilationException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public void setController(AbstractFeedDataFlowController controller) {

    }

    @Override
    public void setFeedLogManager(IFeedLogManager feedLogManager) throws HyracksDataException {

    }

    @Override
    public boolean handleException(Throwable th) {
        return false;
    }

    private void setNextRecordsIterator() {
        FileScanTask task = fileScanTasks.get(nextTaskIndex++);
        InputFile inFile = tableFileIo.newInputFile(task.file().location());
        iterable = Parquet.read(inFile).project(projectedSchema).split(task.start(), task.length())
                .createReaderFunc(fs -> GenericParquetReaders.buildReader(projectedSchema, fs)).build();
        recordsIterator = iterable.iterator();
    }
}
