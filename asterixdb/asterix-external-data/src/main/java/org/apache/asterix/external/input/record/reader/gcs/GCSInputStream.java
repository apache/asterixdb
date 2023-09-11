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
package org.apache.asterix.external.input.record.reader.gcs;

import static org.apache.hyracks.api.util.ExceptionUtils.getMessageOrToString;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.external.input.filter.embedder.IExternalFilterValueEmbedder;
import org.apache.asterix.external.input.record.reader.abstracts.AbstractExternalInputStream;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.google.gcs.GCSUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.util.LogRedactionUtil;

import com.google.cloud.BaseServiceException;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;

public class GCSInputStream extends AbstractExternalInputStream {

    private final Storage client;
    private final String container;
    private static final int MAX_ATTEMPTS = 5; // We try a total of 5 times in case of retryable errors

    public GCSInputStream(Map<String, String> configuration, List<String> filePaths,
            IExternalFilterValueEmbedder valueEmbedder) throws HyracksDataException {
        super(configuration, filePaths, valueEmbedder);
        this.client = buildClient(configuration);
        this.container = configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME);
    }

    @Override
    protected boolean getInputStream() throws IOException {
        String fileName = filePaths.get(nextFileIndex);

        // Have a reference to the input stream to ensure that if GZipInputStream causes an IOException because of
        // reading the header, then the input stream gets closed in the close method
        if (!doGetInputStream(fileName)) {
            return false;
        }

        // Use gzip stream if needed
        if (StringUtils.endsWithIgnoreCase(fileName, ".gz") || StringUtils.endsWithIgnoreCase(fileName, ".gzip")) {
            in = new GZIPInputStream(in, ExternalDataConstants.DEFAULT_BUFFER_SIZE);
        }
        return true;
    }

    /**
     * Get the input stream. If an error is encountered, depending on the error, a retry might be favorable.
     *
     * @return true
     */
    private boolean doGetInputStream(String fileName) throws RuntimeDataException {
        int attempt = 0;
        BlobId blobId = BlobId.of(container, fileName);

        while (attempt < MAX_ATTEMPTS) {
            try {
                Blob blob = client.get(blobId);
                if (blob == null) {
                    // Object not found
                    LOGGER.debug(() -> "Key " + LogRedactionUtil.userData(fileName) + " was not found in container "
                            + container);
                    return false;
                }
                in = new ByteArrayInputStream(blob.getContent());
                break;
            } catch (BaseServiceException ex) {
                if (!ex.isRetryable() || !shouldRetry(++attempt)) {
                    throw new RuntimeDataException(ErrorCode.EXTERNAL_SOURCE_ERROR, getMessageOrToString(ex));
                }
                LOGGER.debug(() -> "Retryable error: " + getMessageOrToString(ex));

                // Backoff for 1 sec for the first 3 attempts, and 2 seconds from there onward
                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(attempt < 3 ? 1 : 2));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            } catch (Exception ex) {
                throw new RuntimeDataException(ErrorCode.EXTERNAL_SOURCE_ERROR, getMessageOrToString(ex));
            }
        }
        return true;
    }

    private boolean shouldRetry(int nextAttempt) {
        return nextAttempt < MAX_ATTEMPTS;
    }

    @Override
    public void close() throws IOException {
        if (in != null) {
            CleanupUtils.close(in, null);
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

    private Storage buildClient(Map<String, String> configuration) throws HyracksDataException {
        try {
            return GCSUtils.buildClient(configuration);
        } catch (CompilationException ex) {
            throw HyracksDataException.create(ex);
        }
    }
}
