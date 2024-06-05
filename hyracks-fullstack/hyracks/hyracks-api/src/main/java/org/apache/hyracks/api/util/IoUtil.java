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
package org.apache.hyracks.api.util;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This util class takes care of creation and deletion of files and directories
 * and throws the appropriate error in case of failure.
 */
public class IoUtil {

    public static final String FILE_NOT_FOUND_MSG = "Deleting non-existing file!";
    public static final FilenameFilter NO_OP_FILTER = (dir, name) -> true;
    private static final Logger LOGGER = LogManager.getLogger();

    private IoUtil() {
    }

    /**
     * Deletes a file
     *
     * @param filePath the file path to be deleted
     * @throws HyracksDataException if the file couldn't be deleted
     */
    public static void delete(Path filePath) throws HyracksDataException {
        delete(filePath.toFile());
    }

    /**
     * Delete a file
     *
     * @param fileRef the file to be deleted
     * @throws HyracksDataException if the file couldn't be deleted
     */
    public static void delete(FileReference fileRef) throws HyracksDataException {
        delete(fileRef.getFile());
    }

    /**
     * Delete a file or directory
     *
     * @param file the file to be deleted
     * @throws HyracksDataException if the file (or directory if exists) couldn't be deleted
     */
    public static void delete(File file) throws HyracksDataException {
        try {
            if (file.isDirectory()) {
                if (!file.exists()) {
                    return;
                } else if (!FileUtils.isSymlink(file)) {
                    cleanDirectory(file);
                }
            }
            Files.delete(file.toPath());
        } catch (NoSuchFileException | FileNotFoundException e) {
            LOGGER.warn(() -> FILE_NOT_FOUND_MSG + ": " + e.getMessage(), e);
        } catch (IOException e) {
            throw HyracksDataException.create(ErrorCode.CANNOT_DELETE_FILE, e, file.getAbsolutePath());
        }
    }

    /**
     * Create a file on disk
     *
     * @param fileRef the file to create
     * @throws HyracksDataException if the file already exists or if it couldn't be created
     */
    public static void create(FileReference fileRef) throws HyracksDataException {
        if (fileRef.getFile().exists()) {
            throw HyracksDataException.create(ErrorCode.FILE_ALREADY_EXISTS, fileRef.getAbsolutePath());
        }
        fileRef.getFile().getParentFile().mkdirs();
        try {
            if (!fileRef.getFile().createNewFile()) {
                throw HyracksDataException.create(ErrorCode.FILE_ALREADY_EXISTS, fileRef.getAbsolutePath());
            }
        } catch (IOException e) {
            throw HyracksDataException.create(ErrorCode.CANNOT_CREATE_FILE, e, fileRef.getAbsolutePath());
        }
    }

    private static void cleanDirectory(final File directory) throws IOException {
        final File[] files = verifiedListFiles(directory);
        for (final File file : files) {
            delete(file);
        }
    }

    private static File[] verifiedListFiles(File directory) throws IOException {
        if (!directory.exists()) {
            final String message = directory + " does not exist";
            throw new IllegalArgumentException(message);
        }

        if (!directory.isDirectory()) {
            final String message = directory + " is not a directory";
            throw new IllegalArgumentException(message);
        }

        final File[] files = directory.listFiles();
        if (files == null) { // null if security restricted
            throw new IOException("Failed to list contents of " + directory);
        }
        return files;
    }

    public static void flushDirectory(File directory) throws IOException {
        flushDirectory(directory.toPath());
    }

    public static void flushDirectory(Path path) throws IOException {
        if (!Files.isDirectory(path)) {
            throw new IOException("Not a directory: " + path);
        }
        if (Files.getFileStore(path).supportsFileAttributeView("posix")) {
            try (FileChannel ch = FileChannel.open(path, StandardOpenOption.READ)) {
                ch.force(true);
            }
        } else {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Unable to flush directory " + path);
            }
        }
    }

    /**
     * Gets a collection of files matching {@code filter} by searching {@code root} directory and
     * all of its subdirectories
     *
     * @param root
     * @param filter
     * @return a collection of matching files
     */
    public static Collection<File> getMatchingFiles(Path root, FilenameFilter filter) {
        if (!Files.isDirectory(root)) {
            throw new IllegalArgumentException("Parameter 'root' is not a directory: " + root);
        }
        Objects.requireNonNull(filter);
        Collection<File> files = new ArrayList<>();
        FileFilter dirOrMatchingFileFilter = file -> file.isDirectory() || filter.accept(file, file.getName());
        collectDirFiles(root.toFile(), dirOrMatchingFileFilter, files);
        return files;
    }

    private static void collectDirFiles(File dir, FileFilter filter, Collection<File> files) {
        File[] matchingFiles = dir.listFiles(filter);
        if (matchingFiles != null) {
            for (File file : matchingFiles) {
                if (file.isDirectory()) {
                    collectDirFiles(file, filter, files);
                } else {
                    files.add(file);
                }
            }
        }
    }

    public static String getFileNameFromPath(String path) {
        return path.substring(path.lastIndexOf('/') + 1);
    }

    public static Collection<FileReference> getMatchingChildren(FileReference root, FilenameFilter filter) {
        if (!root.getFile().isDirectory()) {
            throw new IllegalArgumentException("Parameter 'root' is not a directory: " + root);
        }
        Objects.requireNonNull(filter);
        List<FileReference> files = new ArrayList<>();
        String[] matchingFiles = root.getFile().list(filter);
        if (matchingFiles != null) {
            files.addAll(Arrays.stream(matchingFiles).map(root::getChild).collect(Collectors.toList()));
        }
        return files;
    }

    public static long sizeOfDirectory(final Path path) {
        final AtomicLong size = new AtomicLong(0);
        try {
            Files.walkFileTree(path, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    size.addAndGet(attrs.size());
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) {
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            // This should never happen
            throw new IllegalStateException("Cannot get the size of directory " + path);
        }
        return size.get();
    }
}
