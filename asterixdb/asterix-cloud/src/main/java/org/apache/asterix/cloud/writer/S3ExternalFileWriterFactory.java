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

import static org.apache.asterix.cloud.writer.AbstractCloudExternalFileWriter.isExceedingMaxLength;
import static org.apache.hyracks.api.util.ExceptionUtils.getMessageOrToString;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Random;

import org.apache.asterix.cloud.CloudResettableInputStream;
import org.apache.asterix.cloud.WriterSingleBufferProvider;
import org.apache.asterix.cloud.clients.ICloudBufferedWriter;
import org.apache.asterix.cloud.clients.ICloudClient;
import org.apache.asterix.cloud.clients.aws.s3.S3ClientConfig;
import org.apache.asterix.cloud.clients.aws.s3.S3CloudClient;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.aws.s3.S3Utils;
import org.apache.asterix.runtime.writer.ExternalFileWriterConfiguration;
import org.apache.asterix.runtime.writer.IExternalFileFilterWriterFactoryProvider;
import org.apache.asterix.runtime.writer.IExternalFilePrinter;
import org.apache.asterix.runtime.writer.IExternalFilePrinterFactory;
import org.apache.asterix.runtime.writer.IExternalFileWriter;
import org.apache.asterix.runtime.writer.IExternalFileWriterFactory;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.util.ExceptionUtils;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;

public final class S3ExternalFileWriterFactory implements IExternalFileWriterFactory {
    private static final long serialVersionUID = 4551318140901866805L;
    private static final Logger LOGGER = LogManager.getLogger();
    static final char SEPARATOR = '/';
    public static final IExternalFileFilterWriterFactoryProvider PROVIDER =
            new IExternalFileFilterWriterFactoryProvider() {
                @Override
                public IExternalFileWriterFactory create(ExternalFileWriterConfiguration configuration) {
                    return new S3ExternalFileWriterFactory(configuration);
                }

                @Override
                public char getSeparator() {
                    return SEPARATOR;
                }
            };
    private final Map<String, String> configuration;
    private final SourceLocation pathSourceLocation;
    private final String staticPath;
    private transient S3CloudClient cloudClient;

    private S3ExternalFileWriterFactory(ExternalFileWriterConfiguration externalConfig) {
        configuration = externalConfig.getConfiguration();
        pathSourceLocation = externalConfig.getPathSourceLocation();
        staticPath = externalConfig.getStaticPath();
        cloudClient = null;
    }

    @Override
    public IExternalFileWriter createWriter(IHyracksTaskContext context, IExternalFilePrinterFactory printerFactory)
            throws HyracksDataException {
        buildClient();
        String bucket = configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME);
        IExternalFilePrinter printer = printerFactory.createPrinter();
        IWarningCollector warningCollector = context.getWarningCollector();
        return new S3ExternalFileWriter(printer, cloudClient, bucket, staticPath == null, warningCollector,
                pathSourceLocation);
    }

    private void buildClient() throws HyracksDataException {
        try {
            synchronized (this) {
                if (cloudClient == null) {
                    // only a single client should be built
                    S3ClientConfig config = S3ClientConfig.of(configuration);
                    cloudClient = new S3CloudClient(config, S3Utils.buildAwsS3Client(configuration));
                }
            }
        } catch (CompilationException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public char getSeparator() {
        return SEPARATOR;
    }

    @Override
    public void validate() throws AlgebricksException {
        S3ClientConfig config = S3ClientConfig.of(configuration);
        ICloudClient testClient = new S3CloudClient(config, S3Utils.buildAwsS3Client(configuration));
        String bucket = configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME);

        if (bucket == null || bucket.isEmpty()) {
            throw new CompilationException(ErrorCode.PARAMETERS_REQUIRED,
                    ExternalDataConstants.CONTAINER_NAME_FIELD_NAME);
        }

        try {
            doValidate(testClient, bucket);
        } catch (IOException e) {
            if (e.getCause() instanceof NoSuchBucketException) {
                throw CompilationException.create(ErrorCode.EXTERNAL_SOURCE_CONTAINER_NOT_FOUND, bucket);
            } else {
                throw CompilationException.create(ErrorCode.EXTERNAL_SOURCE_ERROR,
                        ExceptionUtils.getMessageOrToString(e));
            }
        } catch (SdkException e) {
            throw CompilationException.create(ErrorCode.EXTERNAL_SOURCE_ERROR, e, getMessageOrToString(e));
        }
    }

    private void doValidate(ICloudClient testClient, String bucket) throws IOException, AlgebricksException {
        if (staticPath != null) {
            if (isExceedingMaxLength(staticPath, S3ExternalFileWriter.MAX_LENGTH_IN_BYTES)) {
                throw new CompilationException(ErrorCode.WRITE_PATH_LENGTH_EXCEEDS_MAX_LENGTH, pathSourceLocation,
                        staticPath, S3ExternalFileWriter.MAX_LENGTH_IN_BYTES,
                        ExternalDataConstants.KEY_ADAPTER_NAME_AWS_S3);
            }

            if (!testClient.isEmptyPrefix(bucket, staticPath)) {
                // Ensure that the static path is empty
                throw new CompilationException(ErrorCode.DIRECTORY_IS_NOT_EMPTY, pathSourceLocation, staticPath);
            }
        }

        Random random = new Random();
        String pathPrefix = "testFile";
        String path = pathPrefix + random.nextInt();
        while (testClient.exists(bucket, path)) {
            path = pathPrefix + random.nextInt();
        }

        long writeValue = random.nextLong();
        byte[] data = new byte[Long.BYTES];
        LongPointable.setLong(data, 0, writeValue);
        ICloudBufferedWriter writer = testClient.createBufferedWriter(bucket, path);
        CloudResettableInputStream stream = null;
        boolean aborted = false;
        try {
            stream = new CloudResettableInputStream(writer, new WriterSingleBufferProvider());
            stream.write(data, 0, data.length);
        } catch (HyracksDataException e) {
            stream.abort();
            aborted = true;
        } finally {
            if (stream != null && !aborted) {
                stream.finish();
                stream.close();
            }
        }

        try {
            long readValue = LongPointable.getLong(testClient.readAllBytes(bucket, path), 0);
            if (writeValue != readValue) {
                // This should never happen unless S3 is messed up. But log for sanity check
                LOGGER.warn(
                        "The writer can write but the written values wasn't successfully read back (wrote: {}, read:{})",
                        writeValue, readValue);
            }
        } finally {
            // Delete the written file
            testClient.deleteObjects(bucket, Collections.singleton(path));
        }
    }
}
