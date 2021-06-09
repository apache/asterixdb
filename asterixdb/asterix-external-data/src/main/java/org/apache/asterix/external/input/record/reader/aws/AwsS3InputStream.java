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
package org.apache.asterix.external.input.record.reader.aws;

import static org.apache.asterix.external.util.ExternalDataConstants.AwsS3;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.external.input.stream.AbstractMultipleInputStream;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.util.LogRedactionUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class AwsS3InputStream extends AbstractMultipleInputStream {

    private static final Logger LOGGER = LogManager.getLogger();

    // Configuration
    private final String bucket;
    private final int bufferSize;

    private final S3Client s3Client;
    private static final int MAX_RETRIES = 5; // We will retry 5 times in case of internal error from AWS S3 service

    // File fields
    private final List<String> filePaths;
    private int nextFileIndex = 0;

    public AwsS3InputStream(Map<String, String> configuration, List<String> filePaths) throws HyracksDataException {
        this.filePaths = filePaths;
        this.s3Client = buildAwsS3Client(configuration);
        this.bufferSize = ExternalDataUtils.getOrDefaultBufferSize(configuration);
        this.bucket = configuration.get(AwsS3.CONTAINER_NAME_FIELD_NAME);
    }

    @Override
    protected boolean advance() throws IOException {
        // No files to read for this partition
        if (filePaths == null || filePaths.isEmpty()) {
            return false;
        }

        // Finished reading all the files
        if (nextFileIndex >= filePaths.size()) {
            return false;
        }

        // Close the current stream before going to the next one
        if (in != null) {
            CleanupUtils.close(in, null);
        }

        String fileName = filePaths.get(nextFileIndex);
        GetObjectRequest.Builder getObjectBuilder = GetObjectRequest.builder();
        GetObjectRequest getObjectRequest = getObjectBuilder.bucket(bucket).key(fileName).build();

        boolean isAvailableStream = doGetInputStream(getObjectRequest);
        nextFileIndex++;
        if (!isAvailableStream) {
            return advance();
        }

        // Use gzip stream if needed
        if (StringUtils.endsWithIgnoreCase(fileName, ".gz") || StringUtils.endsWithIgnoreCase(fileName, ".gzip")) {
            in = new GZIPInputStream(in, bufferSize);
        }

        // Current file ready, point to the next file
        if (notificationHandler != null) {
            notificationHandler.notifyNewSource();
        }
        return true;
    }

    /**
     * Get the input stream. If an error is encountered, depending on the error code, a retry might be favorable.
     *
     * @return true
     */
    private boolean doGetInputStream(GetObjectRequest request) throws RuntimeDataException {
        int retries = 0;
        while (retries < MAX_RETRIES) {
            try {
                in = s3Client.getObject(request);
                break;
            } catch (NoSuchKeyException ex) {
                LOGGER.debug(() -> "Key " + LogRedactionUtil.userData(request.key()) + " was not found in bucket "
                        + request.bucket());
                return false;
            } catch (S3Exception ex) {
                if (!shouldRetry(ex.awsErrorDetails().errorCode(), retries++)) {
                    throw new RuntimeDataException(ErrorCode.EXTERNAL_SOURCE_ERROR, ex.getMessage());
                }
                LOGGER.debug(() -> "S3 retryable error: " + LogRedactionUtil.userData(ex.getMessage()));

                // Backoff for 1 sec for the first 2 retries, and 2 seconds from there onward
                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(retries < 3 ? 1 : 2));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            } catch (SdkException ex) {
                throw new RuntimeDataException(ErrorCode.EXTERNAL_SOURCE_ERROR, ex.getMessage());
            }
        }
        return true;
    }

    private boolean shouldRetry(String errorCode, int currentRetry) {
        return currentRetry < MAX_RETRIES && AwsS3.isRetryableError(errorCode);
    }

    private S3Client buildAwsS3Client(Map<String, String> configuration) throws HyracksDataException {
        try {
            return ExternalDataUtils.AwsS3.buildAwsS3Client(configuration);
        } catch (CompilationException ex) {
            throw HyracksDataException.create(ex);
        }
    }

    @Override
    public boolean stop() {
        return false;
    }

    @Override
    public boolean handleException(Throwable th) {
        return false;
    }

    @Override
    public void close() throws IOException {
        if (in != null) {
            CleanupUtils.close(in, null);
        }
        if (s3Client != null) {
            CleanupUtils.close(s3Client, null);
        }
    }

    @Override
    public String getStreamName() {
        return getStreamNameAt(nextFileIndex - 1);
    }

    @Override
    public String getPreviousStreamName() {
        return getStreamNameAt(nextFileIndex - 2);
    }

    private String getStreamNameAt(int fileIndex) {
        return fileIndex < 0 || filePaths == null || filePaths.isEmpty() ? "" : filePaths.get(fileIndex);
    }
}
