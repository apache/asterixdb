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

import org.apache.asterix.cloud.writer.S3ExternalFileWriterFactory;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.writer.LocalFSExternalFileWriterFactory;
import org.apache.asterix.external.writer.compressor.GzipExternalFileCompressStreamFactory;
import org.apache.asterix.external.writer.compressor.IExternalFileCompressStreamFactory;
import org.apache.asterix.external.writer.compressor.NoOpExternalFileCompressStreamFactory;
import org.apache.asterix.external.writer.printer.TextualExternalFilePrinterFactory;
import org.apache.asterix.formats.nontagged.CleanJSONPrinterFactoryProvider;
import org.apache.asterix.runtime.writer.ExternalFileWriterConfiguration;
import org.apache.asterix.runtime.writer.IExternalFileFilterWriterFactoryProvider;
import org.apache.asterix.runtime.writer.IExternalFilePrinterFactory;
import org.apache.asterix.runtime.writer.IExternalFileWriterFactory;
import org.apache.hyracks.algebricks.core.algebra.metadata.IWriteDataSink;
import org.apache.hyracks.algebricks.data.IPrinterFactory;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.control.cc.ClusterControllerService;

public class ExternalWriterProvider {
    private static final Map<String, IExternalFileFilterWriterFactoryProvider> CREATOR_MAP;
    private static final Map<String, IExternalFileCompressStreamFactory> STREAM_COMPRESSORS;

    private ExternalWriterProvider() {
    }

    static {
        CREATOR_MAP = new HashMap<>();
        addCreator(ExternalDataConstants.KEY_ADAPTER_NAME_LOCALFS, LocalFSExternalFileWriterFactory.PROVIDER);
        addCreator(ExternalDataConstants.KEY_ADAPTER_NAME_AWS_S3, S3ExternalFileWriterFactory.PROVIDER);

        STREAM_COMPRESSORS = new HashMap<>();
        STREAM_COMPRESSORS.put(ExternalDataConstants.KEY_COMPRESSION_GZIP,
                GzipExternalFileCompressStreamFactory.INSTANCE);
    }

    public static IExternalFileWriterFactory createWriterFactory(ICcApplicationContext appCtx, IWriteDataSink sink,
            String staticPath, SourceLocation pathExpressionLocation) {
        String adapterName = sink.getAdapterName().toLowerCase();
        IExternalFileFilterWriterFactoryProvider creator = CREATOR_MAP.get(adapterName);

        if (creator == null) {
            throw new UnsupportedOperationException("Unsupported adapter " + adapterName);
        }

        return creator.create(createConfiguration(appCtx, sink, staticPath, pathExpressionLocation));
    }

    public static String getFileExtension(IWriteDataSink sink) {
        Map<String, String> configuration = sink.getConfiguration();
        String format = getFormat(configuration);
        String compression = getCompression(configuration);
        return format + (compression.isEmpty() ? "" : "." + compression);
    }

    public static int getMaxResult(IWriteDataSink sink) {
        String maxResultString = sink.getConfiguration().get(ExternalDataConstants.KEY_WRITER_MAX_RESULT);
        if (maxResultString == null) {
            return ExternalDataConstants.WRITER_MAX_RESULT_DEFAULT;
        }
        return Integer.parseInt(maxResultString);
    }

    private static ExternalFileWriterConfiguration createConfiguration(ICcApplicationContext appCtx,
            IWriteDataSink sink, String staticPath, SourceLocation pathExpressionLocation) {
        Map<String, String> params = sink.getConfiguration();
        boolean singleNodeCluster = isSingleNodeCluster(appCtx);

        return new ExternalFileWriterConfiguration(params, pathExpressionLocation, staticPath, singleNodeCluster);
    }

    private static boolean isSingleNodeCluster(ICcApplicationContext appCtx) {
        ClusterControllerService ccs = (ClusterControllerService) appCtx.getServiceContext().getControllerService();
        return ccs.getNodeManager().getIpAddressNodeNameMap().size() == 1;
    }

    private static void addCreator(String adapterName, IExternalFileFilterWriterFactoryProvider creator) {
        IExternalFileFilterWriterFactoryProvider registeredCreator = CREATOR_MAP.get(adapterName.toLowerCase());
        if (registeredCreator != null) {
            throw new IllegalStateException(
                    "Adapter " + adapterName + " is registered to " + registeredCreator.getClass().getName());
        }
        CREATOR_MAP.put(adapterName.toLowerCase(), creator);
    }

    public static IExternalFilePrinterFactory createPrinter(IWriteDataSink sink, Object sourceType) {
        Map<String, String> configuration = sink.getConfiguration();
        String format = configuration.get(ExternalDataConstants.KEY_FORMAT);

        // Only JSON is supported for now
        if (!ExternalDataConstants.FORMAT_JSON_LOWER_CASE.equalsIgnoreCase(format)) {
            throw new UnsupportedOperationException("Unsupported format " + format);
        }

        String compression = getCompression(configuration);
        IExternalFileCompressStreamFactory compressStreamFactory =
                STREAM_COMPRESSORS.getOrDefault(compression, NoOpExternalFileCompressStreamFactory.INSTANCE);

        IPrinterFactory printerFactory = CleanJSONPrinterFactoryProvider.INSTANCE.getPrinterFactory(sourceType);
        return new TextualExternalFilePrinterFactory(printerFactory, compressStreamFactory);
    }

    private static String getFormat(Map<String, String> configuration) {
        return configuration.get(ExternalDataConstants.KEY_FORMAT);
    }

    private static String getCompression(Map<String, String> configuration) {
        return configuration.getOrDefault(ExternalDataConstants.KEY_WRITER_COMPRESSION, "");
    }

    public static char getSeparator(String adapterName) {
        IExternalFileFilterWriterFactoryProvider creator = CREATOR_MAP.get(adapterName.toLowerCase());

        if (creator == null) {
            throw new UnsupportedOperationException("Unsupported adapter " + adapterName);
        }

        return creator.getSeparator();
    }
}
