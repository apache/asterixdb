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

import static org.apache.asterix.external.writer.printer.parquet.ParquetSchemaLazyVisitor.generateSchema;

import java.io.Serializable;

import org.apache.asterix.external.writer.printer.ParquetExternalFilePrinterFactory;
import org.apache.asterix.external.writer.printer.parquet.ParquetSchemaTree;
import org.apache.asterix.runtime.writer.ExternalFileWriter;
import org.apache.asterix.runtime.writer.IExternalFileWriter;
import org.apache.asterix.runtime.writer.IExternalFileWriterFactory;
import org.apache.asterix.runtime.writer.IPathResolver;
import org.apache.hyracks.algebricks.runtime.writers.IExternalWriter;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.parquet.schema.MessageType;

public class ParquetExternalWriterFactory implements Serializable {

    private static final long serialVersionUID = 8971234908711236L;
    private final IExternalFileWriterFactory writerFactory;
    private final int maxResult;
    private final ParquetExternalFilePrinterFactory printerFactory;
    private final IPathResolver resolver;
    private final IHyracksTaskContext ctx;

    public ParquetExternalWriterFactory(IHyracksTaskContext ctx, IExternalFileWriterFactory writerFactory,
            int maxResult, ParquetExternalFilePrinterFactory printerFactory, IPathResolver resolver) {
        this.ctx = ctx;
        this.writerFactory = writerFactory;
        this.maxResult = maxResult;
        this.printerFactory = printerFactory;
        this.resolver = resolver;
    }

    public IExternalWriter createWriter(ParquetSchemaTree.SchemaNode schemaNode) throws HyracksDataException {
        MessageType schema = generateSchema(schemaNode);
        printerFactory.setParquetSchemaString(schema.toString());
        IExternalFileWriter writer = writerFactory.createWriter(ctx, printerFactory);
        return new ExternalFileWriter(resolver, writer, maxResult);
    }

}
