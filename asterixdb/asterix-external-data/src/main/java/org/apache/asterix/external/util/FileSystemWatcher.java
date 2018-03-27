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
package org.apache.asterix.external.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FileSystemWatcher {

    private static final Logger LOGGER = LogManager.getLogger();
    private WatchService watcher;
    private final HashMap<WatchKey, Path> keys;
    private final LinkedList<File> files = new LinkedList<File>();
    private Iterator<File> it;
    private final String expression;
    private FeedLogManager logManager;
    private final List<Path> paths;
    private final boolean isFeed;
    private boolean done;
    private final LinkedList<Path> dirs;

    public FileSystemWatcher(List<Path> inputResources, String expression, boolean isFeed) throws HyracksDataException {
        this.isFeed = isFeed;
        this.keys = isFeed ? new HashMap<WatchKey, Path>() : null;
        this.expression = expression;
        this.paths = inputResources;
        this.dirs = new LinkedList<Path>();
        if (!isFeed) {
            init();
        }
    }

    public synchronized void setFeedLogManager(FeedLogManager feedLogManager) throws HyracksDataException {
        if (logManager == null) {
            this.logManager = feedLogManager;
            init();
        }
    }

    public synchronized void init() throws HyracksDataException {
        try {
            dirs.clear();
            for (Path path : paths) {
                LocalFileSystemUtils.traverse(files, path.toFile(), expression, dirs);
                it = files.iterator();
                if (isFeed) {
                    keys.clear();
                    if (watcher != null) {
                        try {
                            watcher.close();
                        } catch (IOException e) {
                            LOGGER.warn("Failed to close watcher service", e);
                        }
                    }
                    watcher = FileSystems.getDefault().newWatchService();
                    for (Path dirPath : dirs) {
                        register(dirPath);
                    }
                    resume();
                } else {
                    if (files.isEmpty()) {
                        throw new RuntimeDataException(ErrorCode.UTIL_FILE_SYSTEM_WATCHER_NO_FILES_FOUND,
                                path.toString());
                    }
                }
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    /**
     * Register the given directory, and all its sub-directories, with the
     * WatchService.
     */
    private void register(Path dir) throws IOException {
        WatchKey key = dir.register(watcher, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_DELETE,
                StandardWatchEventKinds.ENTRY_MODIFY);
        keys.put(key, dir);
    }

    private synchronized void resume() throws IOException {
        if (logManager == null) {
            return;
        }
        /*
         * Done processing the progress log file. We now have:
         * the files that were completed.
         */

        if (it == null) {
            return;
        }
        while (it.hasNext()) {
            File file = it.next();
            if (logManager.isSplitRead(file.getAbsolutePath())) {
                // File was read completely, remove it from the files list
                it.remove();
            }
        }
        // reset the iterator
        it = files.iterator();
    }

    @SuppressWarnings("unchecked")
    static <T> WatchEvent<T> cast(WatchEvent<?> event) {
        return (WatchEvent<T>) event;
    }

    private synchronized void handleEvents(WatchKey key) throws IOException {
        // get dir associated with the key
        Path dir = keys.get(key);
        if (dir == null) {
            // This should never happen
            LOGGER.warn("WatchKey not recognized!!");
            return;
        }
        for (WatchEvent<?> event : key.pollEvents()) {
            Kind<?> kind = event.kind();
            // An overflow event means that some events were dropped
            if (kind == StandardWatchEventKinds.OVERFLOW) {
                LOGGER.warn("Overflow event. Some events might have been missed");
                // need to read and validate all files.
                init();
                return;
            }

            // Context for directory entry event is the file name of entry
            WatchEvent<Path> ev = cast(event);
            Path name = ev.context();
            Path child = dir.resolve(name);
            // if directory is created then register it and its sub-directories
            if (kind == StandardWatchEventKinds.ENTRY_CREATE) {
                try {
                    if (Files.isDirectory(child, LinkOption.NOFOLLOW_LINKS)) {
                        register(child);
                    } else {
                        // it is a file, add it to the files list.
                        LocalFileSystemUtils.validateAndAdd(child, expression, files);
                    }
                } catch (IOException e) {
                    LOGGER.error(e);
                }
            }
        }
        it = files.iterator();
    }

    public synchronized void close() throws IOException {
        if (!done) {
            if (watcher != null) {
                watcher.close();
                watcher = null;
            }
            done = true;
        }
    }

    // poll is not blocking
    public synchronized File poll() throws IOException {
        if (it.hasNext()) {
            return it.next();
        }
        if (done || !isFeed) {
            return null;
        }
        files.clear();
        it = files.iterator();
        if (keys.isEmpty()) {
            close();
            return null;
        }
        // Read new Events (Polling first to add all available files)
        WatchKey key;
        key = watcher.poll();
        while (key != null) {
            handleEvents(key);
            if (endOfEvents(key)) {
                close();
                return null;
            }
            key = watcher.poll();
        }
        return null;
    }

    // take is blocking
    public File take() throws IOException {
        File next = poll();
        if (next != null) {
            return next;
        }
        if (done || !isFeed) {
            return null;
        }
        // No file was found, wait for the filesystem to push events
        WatchKey key;
        while (!it.hasNext()) {
            try {
                key = watcher.take();
            } catch (InterruptedException x) {
                LOGGER.warn("Feed Closed");
                if (watcher == null) {
                    return null;
                }
                continue;
            } catch (ClosedWatchServiceException e) {
                LOGGER.warn("The watcher has exited");
                if (watcher == null) {
                    return null;
                }
                continue;
            }
            handleEvents(key);
            if (endOfEvents(key)) {
                return null;
            }
        }
        // files were found, re-create the iterator and move it one step
        return it.next();
    }

    private boolean endOfEvents(WatchKey key) {
        // reset key and remove from set if directory no longer accessible
        if (!key.reset()) {
            keys.remove(key);
            if (keys.isEmpty()) {
                return true;
            }
        }
        return false;
    }
}
