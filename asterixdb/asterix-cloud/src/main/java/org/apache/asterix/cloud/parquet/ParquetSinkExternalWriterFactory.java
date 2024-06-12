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

import org.apache.asterix.external.writer.printer.ParquetExternalFilePrinterFactory;
import org.apache.asterix.external.writer.printer.ParquetExternalFilePrinterFactoryProvider;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.writer.IExternalFileWriterFactory;
import org.apache.asterix.runtime.writer.IPathResolverFactory;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntime;
import org.apache.hyracks.algebricks.runtime.operators.base.AbstractPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.writer.WriterPartitionerFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ParquetSinkExternalWriterFactory extends AbstractPushRuntimeFactory {
    private static final long serialVersionUID = -1285997525553685225L;
    private final WriterPartitionerFactory partitionerFactory;
    private final RecordDescriptor inputRecordDesc;
    private final int sourceColumn;
    private final int maxSchemas;
    private final IAType sourceType;
    private final IExternalFileWriterFactory writerFactory;
    private final int maxResult;
    private final ParquetExternalFilePrinterFactoryProvider printerFactoryProvider;
    private final IPathResolverFactory pathResolverFactory;

    public ParquetSinkExternalWriterFactory(WriterPartitionerFactory partitionerFactory,
            RecordDescriptor inputRecordDesc, int sourceColumn, IAType sourceType, int maxSchemas,
            IExternalFileWriterFactory writerFactory, int maxResult,
            ParquetExternalFilePrinterFactoryProvider printerFactoryProvider,
            IPathResolverFactory pathResolverFactory) {
        this.partitionerFactory = partitionerFactory;
        this.inputRecordDesc = inputRecordDesc;
        this.sourceColumn = sourceColumn;
        this.sourceType = sourceType;
        this.maxSchemas = maxSchemas;
        this.writerFactory = writerFactory;
        this.maxResult = maxResult;
        this.printerFactoryProvider = printerFactoryProvider;
        this.pathResolverFactory = pathResolverFactory;
    }

    @Override
    public IPushRuntime[] createPushRuntime(IHyracksTaskContext ctx) throws HyracksDataException {
        ParquetExternalFilePrinterFactory printerFactory =
                (ParquetExternalFilePrinterFactory) printerFactoryProvider.createPrinterFactory();
        ParquetExternalWriterFactory parquetExternalWriterFactory = new ParquetExternalWriterFactory(ctx, writerFactory,
                maxResult, printerFactory, pathResolverFactory.createResolver(ctx));
        ParquetSinkExternalWriterRuntime runtime =
                new ParquetSinkExternalWriterRuntime(sourceColumn, partitionerFactory.createPartitioner(),
                        inputRecordDesc, parquetExternalWriterFactory, sourceType, maxSchemas);
        return new IPushRuntime[] { runtime };
    }
}
