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

import static org.apache.hyracks.api.util.ExceptionUtils.getMessageOrToString;
import static org.apache.hyracks.util.LogRedactionUtil.userData;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.external.input.filter.embedder.IExternalFilterValueEmbedder;
import org.apache.asterix.external.input.record.reader.abstracts.AbstractExternalInputStream;
import org.apache.asterix.external.input.record.reader.stream.AvailableInputStream;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.aws.s3.S3AuthUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.CleanupUtils;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class AwsS3InputStream extends AbstractExternalInputStream {

    private static final int MAX_RETRIES = 5; // We will retry 5 times in case of internal error from AWS S3 service
    private final IApplicationContext ncAppCtx;
    private final String bucket;
    private S3Client s3Client;
    private ResponseInputStream<?> s3InStream;

    public AwsS3InputStream(IApplicationContext ncAppCtx, Map<String, String> configuration, List<String> filePaths,
            IExternalFilterValueEmbedder valueEmbedder) throws HyracksDataException {
        super(configuration, filePaths, valueEmbedder);
        this.ncAppCtx = ncAppCtx;
        this.s3Client = buildAwsS3Client(configuration);
        this.bucket = configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME);
    }

    @Override
    protected boolean getInputStream() throws IOException {
        String fileName = filePaths.get(nextFileIndex);
        GetObjectRequest.Builder getObjectBuilder = GetObjectRequest.builder();
        GetObjectRequest getObjectRequest = getObjectBuilder.bucket(bucket).key(filePaths.get(nextFileIndex)).build();
        // Have a reference to the S3 stream to ensure that if GZipInputStream causes an IOException because of reading
        // the header, then the S3 stream gets closed in the close method
        if (!doGetInputStream(getObjectRequest)) {
            return false;
        }
        // Use gzip stream if needed
        if (StringUtils.endsWithIgnoreCase(fileName, ".gz") || StringUtils.endsWithIgnoreCase(fileName, ".gzip")) {
            in = new GZIPInputStream(new AvailableInputStream(in), ExternalDataConstants.DEFAULT_BUFFER_SIZE);
        }
        return true;
    }

    /**
     * Get the input stream. If an error is encountered, depending on the error code, a retry might be favorable.
     *
     * @return true
     */
    private boolean doGetInputStream(GetObjectRequest request) throws HyracksDataException {
        int retries = 0;
        while (retries < MAX_RETRIES) {
            try {
                s3InStream = s3Client.getObject(request);
                in = s3InStream;
                break;
            } catch (NoSuchKeyException ex) {
                LOGGER.debug(() -> "Key " + userData(request.key()) + " was not found in bucket {}" + request.bucket());
                return false;
            } catch (S3Exception ex) {
                if (S3AuthUtils.isArnAssumedRoleExpiredToken(configuration, ex.awsErrorDetails().errorCode())) {
                    LOGGER.debug(() -> "Expired AWS assume role session, will attempt to refresh the session");
                    rebuildAwsS3Client(configuration);
                    LOGGER.debug(() -> "Successfully refreshed AWS assume role session");
                } else if (shouldRetry(ex.awsErrorDetails().errorCode(), retries++)) {
                    LOGGER.debug(() -> "S3 retryable error: " + userData(ex.getMessage()));
                } else {
                    throw RuntimeDataException.create(ErrorCode.EXTERNAL_SOURCE_ERROR, ex, getMessageOrToString(ex));
                }

                // Backoff for 1 sec for the first 2 retries, and 2 seconds from there onward
                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(retries < 3 ? 1 : 2));
                } catch (InterruptedException e) {
                    throw HyracksDataException.create(e);
                }
            } catch (SdkException ex) {
                throw RuntimeDataException.create(ErrorCode.EXTERNAL_SOURCE_ERROR, ex, getMessageOrToString(ex));
            }
        }
        return true;
    }

    private boolean shouldRetry(String errorCode, int currentRetry) {
        return currentRetry < MAX_RETRIES && S3AuthUtils.isRetryableError(errorCode);
    }

    @Override
    public void close() throws IOException {
        if (in != null) {
            if (s3InStream != null) {
                s3InStream.abort();
            }
            CleanupUtils.close(in, null);
        }
        if (s3Client != null) {
            CleanupUtils.close(s3Client, null);
        }
    }

    @Override
    public boolean stop() {
        try {
            close();
        } catch (IOException e) {
            // Ignore
        }
        return false;
    }

    private S3Client buildAwsS3Client(Map<String, String> configuration) throws HyracksDataException {
        try {
            return S3AuthUtils.buildAwsS3Client(ncAppCtx, configuration);
        } catch (CompilationException ex) {
            throw HyracksDataException.create(ex);
        }
    }

    private void rebuildAwsS3Client(Map<String, String> configuration) throws HyracksDataException {
        s3Client = buildAwsS3Client(configuration);
    }
}
