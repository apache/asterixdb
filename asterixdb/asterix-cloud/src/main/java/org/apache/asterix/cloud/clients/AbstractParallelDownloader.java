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
package org.apache.asterix.cloud.clients;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.util.ExceptionUtils;
import org.apache.hyracks.cloud.util.CloudRetryableRequestUtil;
import org.apache.hyracks.util.ExponentialRetryPolicy;
import org.apache.hyracks.util.IRetryPolicy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AbstractParallelDownloader implements IParallelDownloader {
    private static final Logger LOGGER = LogManager.getLogger();

    public void downloadDirectoriesWithRetry(Collection<FileReference> toDownload) throws HyracksDataException {
        Set<FileReference> failedFiles = new HashSet<>(toDownload);
        IRetryPolicy retryPolicy = new ExponentialRetryPolicy(CloudRetryableRequestUtil.NUMBER_OF_RETRIES,
                CloudRetryableRequestUtil.MAX_DELAY_BETWEEN_RETRIES);
        int attempt = 1;
        while (true) {
            try {
                failedFiles = downloadDirectories(toDownload);

                if (failedFiles.isEmpty()) {
                    return;
                }

                if (!retryPolicy.retry(null)) {
                    LOGGER.error("Exhausted retries ({}) — failed to download {} directories: {}",
                            CloudRetryableRequestUtil.NUMBER_OF_RETRIES, failedFiles.size(), failedFiles);
                    throw HyracksDataException.create(ErrorCode.FAILED_IO_OPERATION);
                }

                LOGGER.warn("Failed to download directories (attempt {}/{}), retrying. Remaining: {}", attempt,
                        CloudRetryableRequestUtil.NUMBER_OF_RETRIES, failedFiles.size());
            } catch (IOException | ExecutionException | InterruptedException e) {
                if (ExceptionUtils.causedByInterrupt(e) && !Thread.currentThread().isInterrupted()) {
                    LOGGER.warn("Lost suppressed interrupt during downloadDirectory retry", e);
                    throw HyracksDataException.create(e);
                }
                try {
                    if (!retryPolicy.retry(e)) {
                        LOGGER.error("Exhausted retries ({}) — failed to download {} directories: {}",
                                CloudRetryableRequestUtil.NUMBER_OF_RETRIES, failedFiles.size(), failedFiles);
                        throw HyracksDataException.create(e);
                    }
                } catch (InterruptedException e1) {
                    throw HyracksDataException.create(e1);
                }
                LOGGER.warn("Failed to downloadDirectories, performing {}/{}", attempt,
                        CloudRetryableRequestUtil.NUMBER_OF_RETRIES, e);
            }
            attempt++;
        }
    }

    protected abstract Set<FileReference> downloadDirectories(Collection<FileReference> toDownload)
            throws ExecutionException, InterruptedException, IOException;
}
