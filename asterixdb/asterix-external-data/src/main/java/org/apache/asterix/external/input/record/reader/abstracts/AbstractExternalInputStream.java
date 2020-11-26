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
package org.apache.asterix.external.input.record.reader.abstracts;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.asterix.external.input.stream.AbstractMultipleInputStream;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AbstractExternalInputStream extends AbstractMultipleInputStream {

    protected static final Logger LOGGER = LogManager.getLogger();

    // Configuration
    protected final Map<String, String> configuration;

    // File fields
    protected final List<String> filePaths;
    protected int nextFileIndex = 0;

    public AbstractExternalInputStream(Map<String, String> configuration, List<String> filePaths) {
        this.configuration = configuration;
        this.filePaths = filePaths;
    }

    @Override
    protected boolean advance() throws IOException {
        // No files to read for this partition
        if (filePaths == null || filePaths.isEmpty()) {
            return false;
        }

        // Finished reading all the files
        if (nextFileIndex >= filePaths.size()) {
            if (in != null) {
                CleanupUtils.close(in, null);
            }
            return false;
        }

        // Close the current stream before going to the next one
        if (in != null) {
            CleanupUtils.close(in, null);
        }

        boolean isAvailableStream = getInputStream();
        nextFileIndex++; // Always point to next file after getting the current stream
        if (!isAvailableStream) {
            return advance();
        }

        if (notificationHandler != null) {
            notificationHandler.notifyNewSource();
        }
        return true;
    }

    protected abstract boolean getInputStream() throws IOException;

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
