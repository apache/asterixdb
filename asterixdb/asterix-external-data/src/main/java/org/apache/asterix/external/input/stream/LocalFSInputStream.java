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
package org.apache.asterix.external.input.stream;

import static org.apache.asterix.common.exceptions.ErrorCode.ASTERIX;
import static org.apache.asterix.common.exceptions.ErrorCode.INPUT_RECORD_READER_CHAR_ARRAY_RECORD_TOO_LARGE;
import static org.apache.asterix.common.exceptions.ErrorCode.RECORD_READER_MALFORMED_INPUT_STREAM;
import static org.apache.hyracks.api.exceptions.ErrorCode.HYRACKS;
import static org.apache.hyracks.api.exceptions.ErrorCode.PARSING_ERROR;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.asterix.common.exceptions.ExceptionUtils;
import org.apache.asterix.external.dataflow.AbstractFeedDataFlowController;
import org.apache.asterix.external.util.FeedLogManager;
import org.apache.asterix.external.util.FileSystemWatcher;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LocalFSInputStream extends AbstractMultipleInputStream {

    private static final Logger LOGGER = LogManager.getLogger();
    private final FileSystemWatcher watcher;
    private File currentFile;
    private String lastFileName = "";

    public LocalFSInputStream(FileSystemWatcher watcher) {
        this.watcher = watcher;
    }

    @Override
    public void setController(AbstractFeedDataFlowController controller) {
        super.setController(controller);
    }

    @Override
    public void setFeedLogManager(FeedLogManager logManager) throws HyracksDataException {
        super.setFeedLogManager(logManager);
        watcher.setFeedLogManager(logManager);
    }

    @Override
    public void close() throws IOException {
        IOException ioe = null;
        if (in != null) {
            try {
                closeFile();
            } catch (Exception e) {
                ioe = new IOException(e);
            }
        }
        try {
            watcher.close();
        } catch (Exception e) {
            if (ioe == null) {
                throw e;
            }
            ioe.addSuppressed(e);
            throw ioe;
        }
    }

    private void closeFile() throws IOException {
        if (in != null) {
            if (logManager != null) {
                logManager.endPartition(currentFile.getAbsolutePath());
            }
            try {
                in.close();
            } finally {
                in = null;
                currentFile = null;
            }
        }
    }

    @Override
    protected boolean advance() throws IOException {
        String tmpLastFileName = "";
        if (currentFile != null) {
            tmpLastFileName = currentFile.getPath();
        }
        closeFile();
        currentFile = watcher.poll();
        if (currentFile == null) {
            if (controller != null) {
                controller.flush();
            }
            currentFile = watcher.take();
        }
        if (currentFile != null) {
            in = new FileInputStream(currentFile);
            lastFileName = tmpLastFileName;
            if (notificationHandler != null) {
                notificationHandler.notifyNewSource();
            }
            return true;
        }
        return false;
    }

    @Override
    public boolean stop() throws Exception {
        closeFile();
        watcher.close();
        return true;
    }

    @Override
    public boolean handleException(Throwable th) {
        if (in == null) {
            return false;
        }
        Throwable root = ExceptionUtils.getRootCause(th);
        if (root instanceof HyracksDataException) {
            HyracksDataException r = (HyracksDataException) root;
            String component = r.getComponent();
            boolean advance = false;
            int errorCode = r.getErrorCode();
            if (ASTERIX.equals(component)) {
                switch (errorCode) {
                    case RECORD_READER_MALFORMED_INPUT_STREAM:
                        logCorruptedInput();
                    case INPUT_RECORD_READER_CHAR_ARRAY_RECORD_TOO_LARGE:
                        advance = true;
                        break;
                    default:
                        break;
                }
            } else if (HYRACKS.equals(component) && errorCode == PARSING_ERROR) {
                logCorruptedInput();
                advance = true;
            }
            if (advance) {
                try {
                    advance();
                    return true;
                } catch (Exception e) {
                    LOGGER.log(Level.WARN, "An exception was thrown while trying to skip a file", e);
                }
            }
        }
        LOGGER.log(Level.WARN, "Failed to recover from failure", th);
        return false;
    }

    @Override
    public String getStreamName() {
        return currentFile == null ? "" : currentFile.getPath();
    }

    @Override
    public String getPreviousStreamName() {
        return lastFileName;
    }

    private void logCorruptedInput() {
        if (currentFile != null) {
            try {
                logManager.logRecord(currentFile.getAbsolutePath(), "Corrupted input file");
            } catch (IOException e) {
                LOGGER.log(Level.WARN, "Filed to write to feed log file", e);
            }
            LOGGER.log(Level.WARN, "Corrupted input file: " + currentFile.getAbsolutePath());
        }
    }
}
