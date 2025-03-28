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
import org.apache.asterix.cloud.clients.aws.s3.S3ClientConfig;
import org.apache.asterix.cloud.clients.aws.s3.S3CloudClient;
import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.aws.s3.S3AuthUtils;
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

import software.amazon.awssdk.core.exception.SdkException;

public final class S3ExternalFileWriterFactory extends AbstractCloudExternalFileWriterFactory<SdkException> {
    private static final long serialVersionUID = 4551318140901866805L;
    static final char SEPARATOR = '/';
    public static final IExternalFileWriterFactoryProvider PROVIDER = new IExternalFileWriterFactoryProvider() {
        @Override
        public IExternalFileWriterFactory create(ExternalFileWriterConfiguration configuration) {
            return new S3ExternalFileWriterFactory(configuration);
        }

        @Override
        public char getSeparator() {
            return SEPARATOR;
        }
    };

    private S3ExternalFileWriterFactory(ExternalFileWriterConfiguration externalConfig) {
        super(externalConfig);
        cloudClient = null;
    }

    @Override
    ICloudClient createCloudClient(IApplicationContext appCtx) throws CompilationException {
        S3ClientConfig config = S3ClientConfig.of(configuration, writeBufferSize);
        return new S3CloudClient(config, S3AuthUtils.buildAwsS3Client(appCtx, configuration),
                ICloudGuardian.NoOpCloudGuardian.INSTANCE);
    }

    @Override
    Optional<SdkException> getSdkException(Throwable ex) {
        return ExceptionUtils.getCauseOfType(ex, SdkException.class);
    }

    @Override
    public IExternalFileWriter createWriter(IHyracksTaskContext context, IExternalPrinterFactory printerFactory)
            throws HyracksDataException {
        IEvaluatorContext evaluatorContext = new EvaluatorContext(context);
        buildClient(((IApplicationContext) context.getJobletContext().getServiceContext().getApplicationContext()));
        String bucket = configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME);
        IExternalPrinter printer = printerFactory.createPrinter(evaluatorContext);
        IWarningCollector warningCollector = context.getWarningCollector();
        return new S3ExternalFileWriter(printer, cloudClient, bucket, staticPath == null, warningCollector,
                pathSourceLocation);
    }

    @Override
    public char getSeparator() {
        return SEPARATOR;
    }
}
