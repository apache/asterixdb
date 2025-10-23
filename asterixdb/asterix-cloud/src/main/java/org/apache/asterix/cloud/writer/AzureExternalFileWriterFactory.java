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
package org.apache.asterix.cloud.writer;

import java.util.Optional;

import org.apache.asterix.cloud.clients.ICloudClient;
import org.apache.asterix.cloud.clients.ICloudGuardian;
import org.apache.asterix.cloud.clients.azure.blobstorage.AzBlobStorageClientConfig;
import org.apache.asterix.cloud.clients.azure.blobstorage.AzBlobStorageCloudClient;
import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.azure.blob_storage.AzureConstants;
import org.apache.asterix.external.util.azure.blob_storage.AzureUtils;
import org.apache.asterix.runtime.writer.ExternalFileWriterConfiguration;
import org.apache.asterix.runtime.writer.IExternalFileWriter;
import org.apache.asterix.runtime.writer.IExternalFileWriterFactory;
import org.apache.asterix.runtime.writer.IExternalFileWriterFactoryProvider;
import org.apache.asterix.runtime.writer.IExternalPrinter;
import org.apache.asterix.runtime.writer.IExternalPrinterFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.EvaluatorContext;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.util.ExceptionUtils;

import com.azure.core.exception.AzureException;

public final class AzureExternalFileWriterFactory extends AbstractCloudExternalFileWriterFactory<AzureException> {
    private static final long serialVersionUID = 4551318140901866805L;
    static final char SEPARATOR = '/';
    public static final IExternalFileWriterFactoryProvider PROVIDER = new IExternalFileWriterFactoryProvider() {
        @Override
        public IExternalFileWriterFactory create(ExternalFileWriterConfiguration configuration) {
            return new AzureExternalFileWriterFactory(configuration);
        }

        @Override
        public char getSeparator() {
            return SEPARATOR;
        }
    };

    private AzureExternalFileWriterFactory(ExternalFileWriterConfiguration externalConfig) {
        super(externalConfig);
        cloudClient = null;
    }

    @Override
    ICloudClient createCloudClient(IApplicationContext appCtx) throws CompilationException {
        AzBlobStorageClientConfig config = AzBlobStorageClientConfig.of(configuration, writeBufferSize);
        return new AzBlobStorageCloudClient(config, AzureUtils.buildAzureBlobClient(appCtx, configuration),
                AzureUtils.buildAzureBlobAsyncClient(appCtx, configuration), ICloudGuardian.NoOpCloudGuardian.INSTANCE);
    }

    @Override
    String getAdapterName() {
        return ExternalDataConstants.KEY_ADAPTER_NAME_AZURE_BLOB;
    }

    @Override
    int getPathMaxLengthInBytes() {
        return AzureConstants.MAX_KEY_LENGTH_IN_BYTES;
    }

    @Override
    Optional<AzureException> getSdkException(Throwable ex) {
        return ExceptionUtils.getCauseOfType(ex, AzureException.class);
    }

    @Override
    public IExternalFileWriter createWriter(IHyracksTaskContext context, IExternalPrinterFactory printerFactory)
            throws HyracksDataException {
        IEvaluatorContext evaluatorContext = new EvaluatorContext(context);
        buildClient(((IApplicationContext) context.getJobletContext().getServiceContext().getApplicationContext()));
        String bucket = configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME);
        IExternalPrinter printer = printerFactory.createPrinter(evaluatorContext);
        IWarningCollector warningCollector = context.getWarningCollector();
        return new AzureExternalFileWriter(printer, cloudClient, bucket, staticPath == null, warningCollector,
                pathSourceLocation);
    }

    @Override
    public char getSeparator() {
        return SEPARATOR;
    }
}
