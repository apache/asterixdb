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
package org.apache.asterix.metadata.provider;

import java.util.HashMap;
import java.util.Map;
import java.util.zip.Deflater;

import org.apache.asterix.cloud.parquet.ParquetSinkExternalWriterFactory;
import org.apache.asterix.cloud.writer.GCSExternalFileWriterFactory;
import org.apache.asterix.cloud.writer.S3ExternalFileWriterFactory;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.external.writer.HDFSExternalFileWriterFactory;
import org.apache.asterix.external.writer.LocalFSExternalFileWriterFactory;
import org.apache.asterix.external.writer.compressor.GzipExternalFileCompressStreamFactory;
import org.apache.asterix.external.writer.compressor.IExternalFileCompressStreamFactory;
import org.apache.asterix.external.writer.compressor.NoOpExternalFileCompressStreamFactory;
import org.apache.asterix.external.writer.printer.CsvExternalFilePrinterFactory;
import org.apache.asterix.external.writer.printer.ParquetExternalFilePrinterFactory;
import org.apache.asterix.external.writer.printer.ParquetExternalFilePrinterFactoryProvider;
import org.apache.asterix.external.writer.printer.TextualExternalFilePrinterFactory;
import org.apache.asterix.formats.nontagged.CSVPrinterFactoryProvider;
import org.apache.asterix.formats.nontagged.CleanJSONPrinterFactoryProvider;
import org.apache.asterix.metadata.declared.IExternalWriteDataSink;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
import org.apache.asterix.runtime.writer.ExternalFileWriterConfiguration;
import org.apache.asterix.runtime.writer.ExternalFileWriterFactory;
import org.apache.asterix.runtime.writer.IExternalFileWriterFactory;
import org.apache.asterix.runtime.writer.IExternalFileWriterFactoryProvider;
import org.apache.asterix.runtime.writer.IExternalPrinterFactory;
import org.apache.asterix.runtime.writer.PathResolverFactory;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.metadata.IWriteDataSink;
import org.apache.hyracks.algebricks.data.IPrinterFactory;
import org.apache.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.operators.writer.SinkExternalWriterRuntimeFactory;
import org.apache.hyracks.algebricks.runtime.operators.writer.WriterPartitionerFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.util.StorageUtil;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class ExternalWriterProvider {
    private static final Map<String, IExternalFileWriterFactoryProvider> CREATOR_MAP;

    private ExternalWriterProvider() {
    }

    static {
        CREATOR_MAP = new HashMap<>();
        addCreator(ExternalDataConstants.KEY_ADAPTER_NAME_LOCALFS, LocalFSExternalFileWriterFactory.PROVIDER);
        addCreator(ExternalDataConstants.KEY_ADAPTER_NAME_AWS_S3, S3ExternalFileWriterFactory.PROVIDER);
        addCreator(ExternalDataConstants.KEY_ADAPTER_NAME_GCS, GCSExternalFileWriterFactory.PROVIDER);
        addCreator(ExternalDataConstants.KEY_ADAPTER_NAME_HDFS, HDFSExternalFileWriterFactory.PROVIDER);
    }

    private static IExternalFileWriterFactory createWriterFactory(ICcApplicationContext appCtx, IWriteDataSink sink,
            String staticPath, SourceLocation pathExpressionLocation) {
        String adapterName = sink.getAdapterName().toLowerCase();
        IExternalFileWriterFactoryProvider creator = CREATOR_MAP.get(adapterName);

        if (creator == null) {
            throw new UnsupportedOperationException("Unsupported adapter " + adapterName);
        }

        return creator.create(createConfiguration(appCtx, sink, staticPath, pathExpressionLocation));
    }

    private static String getFileExtension(IWriteDataSink sink) {
        Map<String, String> configuration = sink.getConfiguration();
        String format = getFormat(configuration);
        String compression = getCompression(configuration);
        return format + (compression.isEmpty() ? "" : "." + compression);
    }

    private static int getMaxResult(IWriteDataSink sink) {
        String maxResultString = sink.getConfiguration().get(ExternalDataConstants.KEY_WRITER_MAX_RESULT);
        if (maxResultString == null) {
            return ExternalDataConstants.WRITER_MAX_RESULT_DEFAULT;
        }
        return Integer.parseInt(maxResultString);
    }

    private static int getMaxParquetSchema(Map<String, String> conf) {
        String maxResultString = conf.get(ExternalDataConstants.PARQUET_MAX_SCHEMAS_KEY);
        if (maxResultString == null) {
            return ExternalDataConstants.PARQUET_MAX_SCHEMAS_DEFAULT_VALUE;
        }
        return Integer.parseInt(maxResultString);
    }

    private static ExternalFileWriterConfiguration createConfiguration(ICcApplicationContext appCtx,
            IWriteDataSink sink, String staticPath, SourceLocation pathExpressionLocation) {
        Map<String, String> params = sink.getConfiguration();
        boolean singleNodeCluster = isSingleNodeCluster(appCtx);
        int copyToWriteBufferSize = appCtx.getCompilerProperties().getCopyToWriteBufferSize();

        return new ExternalFileWriterConfiguration(params, pathExpressionLocation, staticPath, singleNodeCluster,
                copyToWriteBufferSize);
    }

    private static boolean isSingleNodeCluster(ICcApplicationContext appCtx) {
        ClusterControllerService ccs = (ClusterControllerService) appCtx.getServiceContext().getControllerService();
        return ccs.getNodeManager().getIpAddressNodeNameMap().size() == 1;
    }

    private static void addCreator(String adapterName, IExternalFileWriterFactoryProvider creator) {
        IExternalFileWriterFactoryProvider registeredCreator = CREATOR_MAP.get(adapterName.toLowerCase());
        if (registeredCreator != null) {
            throw new IllegalStateException(
                    "Adapter " + adapterName + " is registered to " + registeredCreator.getClass().getName());
        }
        CREATOR_MAP.put(adapterName.toLowerCase(), creator);
    }

    public static IPushRuntimeFactory getWriteFileRuntime(ICcApplicationContext appCtx, IWriteDataSink sink,
            Object sourceType, ILogicalExpression staticPathExpr, SourceLocation pathSourceLocation,
            IScalarEvaluatorFactory dynamicPathEvalFactory, RecordDescriptor inputDesc, int sourceColumn,
            int[] partitionColumns, IBinaryComparatorFactory[] partitionComparatorFactories)
            throws AlgebricksException {
        String staticPath = staticPathExpr != null ? ConstantExpressionUtil.getStringConstant(staticPathExpr) : null;
        IExternalFileWriterFactory fileWriterFactory =
                ExternalWriterProvider.createWriterFactory(appCtx, sink, staticPath, pathSourceLocation);
        fileWriterFactory.validate();
        String fileExtension = ExternalWriterProvider.getFileExtension(sink);
        int maxResult = ExternalWriterProvider.getMaxResult(sink);

        Map<String, String> configuration = sink.getConfiguration();
        String format = configuration.get(ExternalDataConstants.KEY_FORMAT);

        // Check for supported formats
        if (!ExternalDataConstants.WRITER_SUPPORTED_FORMATS.contains(format.toLowerCase())) {
            throw new UnsupportedOperationException("Unsupported format " + format);
        }

        String compression = getCompression(configuration);
        WriterPartitionerFactory partitionerFactory =
                new WriterPartitionerFactory(partitionColumns, partitionComparatorFactories);
        PathResolverFactory pathResolverFactory = new PathResolverFactory(fileWriterFactory, fileExtension,
                dynamicPathEvalFactory, staticPath, pathSourceLocation);
        IPrinterFactory printerFactory;
        IExternalFileCompressStreamFactory compressStreamFactory;
        IExternalPrinterFactory externalPrinterFactory;
        ExternalFileWriterFactory writerFactory;
        switch (format) {
            case ExternalDataConstants.FORMAT_JSON_LOWER_CASE:
                compressStreamFactory = createCompressionStreamFactory(appCtx, compression, configuration);
                printerFactory = CleanJSONPrinterFactoryProvider.INSTANCE.getPrinterFactory(sourceType);
                externalPrinterFactory = new TextualExternalFilePrinterFactory(printerFactory, compressStreamFactory);
                writerFactory = new ExternalFileWriterFactory(fileWriterFactory, externalPrinterFactory,
                        pathResolverFactory, maxResult);

                return new SinkExternalWriterRuntimeFactory(sourceColumn, partitionColumns,
                        partitionComparatorFactories, inputDesc, writerFactory);
            case ExternalDataConstants.FORMAT_CSV_LOWER_CASE:
                compressStreamFactory = createCompressionStreamFactory(appCtx, compression, configuration);
                if (sink instanceof IExternalWriteDataSink) {
                    ARecordType itemType = ((IExternalWriteDataSink) sink).getItemType();
                    if (itemType != null) {
                        printerFactory =
                                CSVPrinterFactoryProvider
                                        .createInstance(itemType, sink.getConfiguration(),
                                                ((IExternalWriteDataSink) sink).getSourceLoc())
                                        .getPrinterFactory(sourceType);
                        externalPrinterFactory =
                                new CsvExternalFilePrinterFactory(printerFactory, compressStreamFactory);
                        writerFactory = new ExternalFileWriterFactory(fileWriterFactory, externalPrinterFactory,
                                pathResolverFactory, maxResult);
                        return new SinkExternalWriterRuntimeFactory(sourceColumn, partitionColumns,
                                partitionComparatorFactories, inputDesc, writerFactory);
                    } else {
                        throw new CompilationException(ErrorCode.INVALID_CSV_SCHEMA);
                    }
                } else {
                    throw new CompilationException(ErrorCode.INVALID_CSV_SCHEMA);
                }

            case ExternalDataConstants.FORMAT_PARQUET:

                CompressionCodecName compressionCodecName;
                if (compression == null || compression.equals("") || compression.equals("none")) {
                    compressionCodecName = CompressionCodecName.UNCOMPRESSED;
                } else {
                    compressionCodecName = CompressionCodecName.valueOf(compression.toUpperCase());
                }

                String rowGroupSizeString = getRowGroupSize(configuration);
                String pageSizeString = getPageSize(configuration);

                long rowGroupSize = StorageUtil.getByteValue(rowGroupSizeString);
                int pageSize = (int) StorageUtil.getByteValue(pageSizeString);
                ParquetProperties.WriterVersion writerVersion = getParquetWriterVersion(configuration);

                if (configuration.get(ExternalDataConstants.PARQUET_SCHEMA_KEY) != null) {
                    String parquetSchemaString = configuration.get(ExternalDataConstants.PARQUET_SCHEMA_KEY);
                    ParquetExternalFilePrinterFactory parquetPrinterFactory =
                            new ParquetExternalFilePrinterFactory(compressionCodecName, parquetSchemaString,
                                    (IAType) sourceType, rowGroupSize, pageSize, writerVersion);

                    ExternalFileWriterFactory parquetWriterFactory = new ExternalFileWriterFactory(fileWriterFactory,
                            parquetPrinterFactory, pathResolverFactory, maxResult);
                    return new SinkExternalWriterRuntimeFactory(sourceColumn, partitionColumns,
                            partitionComparatorFactories, inputDesc, parquetWriterFactory);
                }

                int maxSchemas = ExternalWriterProvider.getMaxParquetSchema(configuration);
                ParquetExternalFilePrinterFactoryProvider printerFactoryProvider =
                        new ParquetExternalFilePrinterFactoryProvider(compressionCodecName, (IAType) sourceType,
                                rowGroupSize, pageSize, writerVersion);
                return new ParquetSinkExternalWriterFactory(partitionerFactory, inputDesc, sourceColumn,
                        (IAType) sourceType, maxSchemas, fileWriterFactory, maxResult, printerFactoryProvider,
                        pathResolverFactory);

            default:
                throw new UnsupportedOperationException("Unsupported format " + format);
        }

    }

    private static ParquetProperties.WriterVersion getParquetWriterVersion(Map<String, String> configuration) {
        String writerVersionString = configuration.getOrDefault(ExternalDataConstants.PARQUET_WRITER_VERSION_KEY,
                ExternalDataConstants.PARQUET_WRITER_VERSION_VALUE_1);
        if (writerVersionString.equals(ExternalDataConstants.PARQUET_WRITER_VERSION_VALUE_2)) {
            return ParquetProperties.WriterVersion.PARQUET_2_0;
        }
        return ParquetProperties.WriterVersion.PARQUET_1_0;
    }

    private static String getRowGroupSize(Map<String, String> configuration) {
        return configuration.getOrDefault(ExternalDataConstants.KEY_PARQUET_ROW_GROUP_SIZE,
                ExternalDataConstants.PARQUET_DEFAULT_ROW_GROUP_SIZE);
    }

    private static String getPageSize(Map<String, String> configuration) {
        return configuration.getOrDefault(ExternalDataConstants.KEY_PARQUET_PAGE_SIZE,
                ExternalDataConstants.PARQUET_DEFAULT_PAGE_SIZE);
    }

    private static String getFormat(Map<String, String> configuration) {
        return configuration.get(ExternalDataConstants.KEY_FORMAT);
    }

    private static String getCompression(Map<String, String> configuration) {
        return configuration.getOrDefault(ExternalDataConstants.KEY_WRITER_COMPRESSION, "");
    }

    public static char getSeparator(String adapterName) {
        IExternalFileWriterFactoryProvider creator = CREATOR_MAP.get(adapterName.toLowerCase());

        if (creator == null) {
            throw new UnsupportedOperationException("Unsupported adapter " + adapterName);
        }

        return creator.getSeparator();
    }

    private static IExternalFileCompressStreamFactory createCompressionStreamFactory(ICcApplicationContext appCtx,
            String compression, Map<String, String> configuration) {
        if (ExternalDataUtils.isGzipCompression(compression)) {
            return createGzipStreamFactory(appCtx, configuration);
        }
        return NoOpExternalFileCompressStreamFactory.INSTANCE;
    }

    private static GzipExternalFileCompressStreamFactory createGzipStreamFactory(ICcApplicationContext appCtx,
            Map<String, String> configuration) {
        int compressionLevel = Deflater.DEFAULT_COMPRESSION;
        String gzipCompressionLevel = configuration.get(ExternalDataConstants.KEY_COMPRESSION_GZIP_COMPRESSION_LEVEL);
        if (gzipCompressionLevel != null) {
            compressionLevel = Integer.parseInt(gzipCompressionLevel);
        }
        return GzipExternalFileCompressStreamFactory.create(compressionLevel,
                appCtx.getCompilerProperties().getFrameSize());
    }
}
