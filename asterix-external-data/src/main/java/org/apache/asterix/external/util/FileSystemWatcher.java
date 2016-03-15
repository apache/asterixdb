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

import org.apache.asterix.external.dataflow.AbstractFeedDataFlowController;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class FileSystemWatcher {

    private static final Logger LOGGER = Logger.getLogger(FileSystemWatcher.class.getName());
    private final WatchService watcher;
    private final HashMap<WatchKey, Path> keys;
    private final LinkedList<File> files = new LinkedList<File>();
    private Iterator<File> it;
    private final String expression;
    private FeedLogManager logManager;
    private final Path path;
    private final boolean isFeed;
    private boolean done;
    private File current;
    private AbstractFeedDataFlowController controller;

    public FileSystemWatcher(Path inputResource, String expression, boolean isFeed) throws HyracksDataException {
        try {
            this.watcher = isFeed ? FileSystems.getDefault().newWatchService() : null;
            this.keys = isFeed ? new HashMap<WatchKey, Path>() : null;
            this.expression = expression;
            this.path = inputResource;
            this.isFeed = isFeed;
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    public void setFeedLogManager(FeedLogManager feedLogManager) {
        this.logManager = feedLogManager;
    }

    public void init() throws HyracksDataException {
        try {
            LinkedList<Path> dirs = null;
            dirs = new LinkedList<Path>();
            LocalFileSystemUtils.traverse(files, path.toFile(), expression, dirs);
            it = files.iterator();
            if (isFeed) {
                for (Path path : dirs) {
                    register(path);
                }
                resume();
            }
        } catch (IOException e) {
            throw new HyracksDataException(e);
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

    private void resume() throws IOException {
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

    private void handleEvents(WatchKey key) {
        // get dir associated with the key
        Path dir = keys.get(key);
        if (dir == null) {
            // This should never happen
            if (LOGGER.isEnabledFor(Level.WARN)) {
                LOGGER.warn("WatchKey not recognized!!");
            }
            return;
        }
        for (WatchEvent<?> event : key.pollEvents()) {
            Kind<?> kind = event.kind();
            // TODO: Do something about overflow events
            // An overflow event means that some events were dropped
            if (kind == StandardWatchEventKinds.OVERFLOW) {
                if (LOGGER.isEnabledFor(Level.WARN)) {
                    LOGGER.warn("Overflow event. Some events might have been missed");
                }
                continue;
            }

            // Context for directory entry event is the file name of entry
            WatchEvent<Path> ev = cast(event);
            Path name = ev.context();
            Path child = dir.resolve(name);
            // if directory is created then register it and its sub-directories
            if ((kind == StandardWatchEventKinds.ENTRY_CREATE)) {
                try {
                    if (Files.isDirectory(child, LinkOption.NOFOLLOW_LINKS)) {
                        register(child);
                    } else {
                        // it is a file, add it to the files list.
                        LocalFileSystemUtils.validateAndAdd(child, expression, files);
                    }
                } catch (IOException e) {
                    if (LOGGER.isEnabledFor(Level.ERROR)) {
                        LOGGER.error(e);
                    }
                }
            }
        }
    }

    public void close() throws IOException {
        if (!done) {
            if (watcher != null) {
                watcher.close();
            }
            if (logManager != null) {
                if (current != null) {
                    logManager.startPartition(current.getAbsolutePath());
                    logManager.endPartition();
                }
                logManager.close();
                current = null;
            }
            done = true;
        }
    }

    public File next() throws IOException {
        if ((current != null) && (logManager != null)) {
            logManager.startPartition(current.getAbsolutePath());
            logManager.endPartition();
        }
        current = it.next();
        return current;
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

    public boolean hasNext() throws HyracksDataException {
        if (it.hasNext()) {
            return true;
        }
        if (done || !isFeed) {
            return false;
        }
        files.clear();
        if (keys.isEmpty()) {
            return false;
        }
        // Read new Events (Polling first to add all available files)
        WatchKey key;
        key = watcher.poll();
        while (key != null) {
            handleEvents(key);
            if (endOfEvents(key)) {
                return false;
            }
            key = watcher.poll();
        }
        // No file was found, wait for the filesystem to push events
        if (controller != null) {
            controller.flush();
        }
        while (files.isEmpty()) {
            try {
                key = watcher.take();
            } catch (InterruptedException x) {
                if (LOGGER.isEnabledFor(Level.WARN)) {
                    LOGGER.warn("Feed Closed");
                }
                return false;
            } catch (ClosedWatchServiceException e) {
                if (LOGGER.isEnabledFor(Level.WARN)) {
                    LOGGER.warn("The watcher has exited");
                }
                return false;
            }
            handleEvents(key);
            if (endOfEvents(key)) {
                return false;
            }
        }
        // files were found, re-create the iterator and move it one step
        it = files.iterator();
        return it.hasNext();
    }

    public void setController(AbstractFeedDataFlowController controller) {
        this.controller = controller;
    }
}
