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
import java.util.Optional;
import java.util.Random;

import org.apache.asterix.cloud.IWriteBufferProvider;
import org.apache.asterix.cloud.WriterSingleBufferProvider;
import org.apache.asterix.cloud.clients.ICloudClient;
import org.apache.asterix.cloud.clients.ICloudWriter;
import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.runtime.writer.ExternalFileWriterConfiguration;
import org.apache.asterix.runtime.writer.IExternalFileWriterFactory;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

abstract class AbstractCloudExternalFileWriterFactory<T extends Throwable> implements IExternalFileWriterFactory {
    private static final long serialVersionUID = -6204498482419719403L;
    private static final Logger LOGGER = LogManager.getLogger();

    protected final Map<String, String> configuration;
    protected final SourceLocation pathSourceLocation;
    protected final String staticPath;
    protected final int writeBufferSize;
    protected transient ICloudClient cloudClient;

    AbstractCloudExternalFileWriterFactory(ExternalFileWriterConfiguration externalConfig) {
        configuration = externalConfig.getConfiguration();
        pathSourceLocation = externalConfig.getPathSourceLocation();
        staticPath = externalConfig.getStaticPath();
        writeBufferSize = externalConfig.getWriteBufferSize();
    }

    abstract ICloudClient createCloudClient(IApplicationContext appCtx) throws CompilationException;

    /**
     * Certain failures are wrapped in our exceptions as IO failures, this method checks if the original failure
     * is reported from the external SDK, and if so, returns it. This is to ensure that the failure
     * reported by the external SDK is the one reported back as the issue.
     *
     * @param ex failure thrown
     * @return the throwable if it is an SDK exception, null otherwise
     */
    abstract Optional<T> getSdkException(Throwable ex);

    final void buildClient(IApplicationContext appCtx) throws HyracksDataException {
        try {
            synchronized (this) {
                if (cloudClient == null) {
                    cloudClient = createCloudClient(appCtx);
                }
            }
        } catch (CompilationException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public final void validate(IApplicationContext appCtx) throws AlgebricksException {
        ICloudClient testClient = createCloudClient(appCtx);
        String bucket = configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME);

        if (bucket == null || bucket.isEmpty()) {
            throw new CompilationException(ErrorCode.PARAMETERS_REQUIRED,
                    ExternalDataConstants.CONTAINER_NAME_FIELD_NAME);
        }

        try {
            doValidate(testClient, bucket);
        } catch (Throwable e) {
            Optional<T> sdkException = getSdkException(e);
            if (sdkException.isPresent()) {
                Throwable actualException = sdkException.get();
                throw CompilationException.create(ErrorCode.EXTERNAL_SINK_ERROR, actualException,
                        getMessageOrToString(actualException));
            }
            if (e instanceof AlgebricksException algebricksException) {
                throw algebricksException;
            }
            throw CompilationException.create(ErrorCode.EXTERNAL_SINK_ERROR, e, getMessageOrToString(e));
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

        // do not validate write permissions if specified by the user not to do so
        String validateWritePermissions = configuration
                .getOrDefault(ExternalDataConstants.KEY_VALIDATE_WRITE_PERMISSION, Boolean.TRUE.toString());
        if (!Boolean.parseBoolean(validateWritePermissions)) {
            return;
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
        IWriteBufferProvider bufferProvider = new WriterSingleBufferProvider(testClient.getWriteBufferSize());
        ICloudWriter writer = testClient.createWriter(bucket, path, bufferProvider);
        boolean aborted = false;
        try {
            writer.write(data, 0, data.length);
        } catch (HyracksDataException e) {
            writer.abort();
            aborted = true;
            throw e;
        } finally {
            if (writer != null && !aborted) {
                writer.finish();
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
