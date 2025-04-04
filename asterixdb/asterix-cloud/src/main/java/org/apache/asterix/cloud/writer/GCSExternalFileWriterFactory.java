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

import java.io.IOException;

import org.apache.asterix.cloud.clients.ICloudClient;
import org.apache.asterix.cloud.clients.ICloudGuardian;
import org.apache.asterix.cloud.clients.google.gcs.GCSClientConfig;
import org.apache.asterix.cloud.clients.google.gcs.GCSCloudClient;
import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.google.gcs.GCSAuthUtils;
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

import com.google.cloud.BaseServiceException;
import com.google.cloud.storage.StorageException;

public final class GCSExternalFileWriterFactory extends AbstractCloudExternalFileWriterFactory {
    private static final long serialVersionUID = 1L;
    static final char SEPARATOR = '/';
    public static final IExternalFileWriterFactoryProvider PROVIDER = new IExternalFileWriterFactoryProvider() {
        @Override
        public IExternalFileWriterFactory create(ExternalFileWriterConfiguration configuration) {
            return new GCSExternalFileWriterFactory(configuration);
        }

        @Override
        public char getSeparator() {
            return SEPARATOR;
        }
    };

    private GCSExternalFileWriterFactory(ExternalFileWriterConfiguration externalConfig) {
        super(externalConfig);
        cloudClient = null;
    }

    @Override
    ICloudClient createCloudClient(IApplicationContext appCtx) throws CompilationException {
        GCSClientConfig config = GCSClientConfig.of(configuration, writeBufferSize);
        return new GCSCloudClient(config, GCSAuthUtils.buildClient(appCtx, configuration),
                ICloudGuardian.NoOpCloudGuardian.INSTANCE);
    }

    @Override
    boolean isNoContainerFoundException(IOException e) {
        return e.getCause() instanceof StorageException;
    }

    @Override
    boolean isSdkException(Throwable e) {
        return e instanceof BaseServiceException;
    }

    @Override
    public IExternalFileWriter createWriter(IHyracksTaskContext context, IExternalPrinterFactory printerFactory)
            throws HyracksDataException {
        IEvaluatorContext evaluatorContext = new EvaluatorContext(context);
        buildClient(((IApplicationContext) context.getJobletContext().getServiceContext().getApplicationContext()));
        String bucket = configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME);
        IExternalPrinter printer = printerFactory.createPrinter(evaluatorContext);
        IWarningCollector warningCollector = context.getWarningCollector();
        return new GCSExternalFileWriter(printer, cloudClient, bucket, staticPath == null, warningCollector,
                pathSourceLocation);
    }

    @Override
    public char getSeparator() {
        return SEPARATOR;
    }
}
