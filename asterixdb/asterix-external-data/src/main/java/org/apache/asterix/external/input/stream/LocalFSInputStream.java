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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import org.apache.asterix.external.api.AsterixInputStream;
import org.apache.asterix.external.dataflow.AbstractFeedDataFlowController;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.FeedLogManager;
import org.apache.asterix.external.util.FileSystemWatcher;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.std.file.FileSplit;
import org.apache.log4j.Logger;

public class LocalFSInputStream extends AsterixInputStream {

    private static final Logger LOGGER = Logger.getLogger(LocalFSInputStream.class.getName());
    private final Path path;
    private final FileSystemWatcher watcher;
    private FileInputStream in;
    private byte lastByte;
    private File currentFile;

    public LocalFSInputStream(final FileSplit[] fileSplits, final IHyracksTaskContext ctx,
            final Map<String, String> configuration, final int partition, final String expression, final boolean isFeed)
            throws IOException {
        this.path = fileSplits[partition].getLocalFile().getFile().toPath();
        this.watcher = new FileSystemWatcher(path, expression, isFeed);
        this.watcher.init();
    }

    @Override
    public void setFeedLogManager(FeedLogManager logManager) {
        super.setFeedLogManager(logManager);
        watcher.setFeedLogManager(logManager);
    }

    @Override
    public void setController(AbstractFeedDataFlowController controller) {
        super.setController(controller);
        watcher.setController(controller);
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
            try {
                in.close();
            } finally {
                in = null;
                currentFile = null;
            }
        }
    }

    /**
     * Closes the current input stream and opens the next one, if any.
     */
    private boolean advance() throws IOException {
        closeFile();
        if (watcher.hasNext()) {
            currentFile = watcher.next();
            in = new FileInputStream(currentFile);
            return true;
        }
        return false;
    }

    @Override
    public int read() throws IOException {
        throw new HyracksDataException(
                "read() is not supported with this stream. use read(byte[] b, int off, int len)");
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (in == null) {
            if (!advance()) {
                return -1;
            }
        }
        int result = in.read(b, off, len);
        while ((result < 0) && advance()) {
            // return a new line at the end of every file <--Might create problems for some cases
            // depending on the parser implementation-->
            if ((lastByte != ExternalDataConstants.BYTE_LF) && (lastByte != ExternalDataConstants.BYTE_LF)) {
                lastByte = ExternalDataConstants.BYTE_LF;
                b[off] = ExternalDataConstants.BYTE_LF;
                return 1;
            }
            // recursive call
            result = in.read(b, off, len);
        }
        if (result > 0) {
            lastByte = b[(off + result) - 1];
        }
        return result;
    }

    @Override
    public boolean stop() throws Exception {
        watcher.close();
        return true;
    }

    @Override
    public boolean handleException(Throwable th) {
        if (in == null) {
            return false;
        }
        if (th instanceof IOException) {
            // TODO: Change from string check to exception type
            if (th.getCause().getMessage().contains("Malformed input stream")) {
                if (currentFile != null) {
                    try {
                        logManager.logRecord(currentFile.getAbsolutePath(), "Corrupted input file");
                    } catch (IOException e) {
                        LOGGER.warn("Filed to write to feed log file", e);
                    }
                    LOGGER.warn("Corrupted input file: " + currentFile.getAbsolutePath());
                }
                try {
                    advance();
                    return true;
                } catch (Exception e) {
                    return false;
                }
            } else {
                try {
                    watcher.init();
                } catch (IOException e) {
                    LOGGER.warn("Failed to initialize watcher during failure recovery", e);
                    return false;
                }
            }
            return true;
        }
        return false;
    }
}
