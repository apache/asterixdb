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
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;

public class LocalFileSystemUtils {

    public static void traverse(final List<File> files, File root, final String expression, final LinkedList<Path> dirs)
            throws IOException {
        final Path path = root.toPath();
        if (!Files.exists(path)) {
            throw new RuntimeDataException(ErrorCode.UTIL_LOCAL_FILE_SYSTEM_UTILS_PATH_NOT_FOUND, path.toString());
        }
        if (!Files.isDirectory(path)) {
            validateAndAdd(path, expression, files);
        }
        Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path path, BasicFileAttributes attrs) throws IOException {
                if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
                    return FileVisitResult.TERMINATE;
                }
                if (Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
                    if (dirs != null) {
                        dirs.add(path);
                    }
                    //get immediate children files
                    File[] content = path.toFile().listFiles();
                    for (File file : content) {
                        if (!file.isDirectory()) {
                            validateAndAdd(file.toPath(), expression, files);
                        }
                    }
                } else {
                    // Path is a file, add to list of files if it matches the expression
                    validateAndAdd(path, expression, files);
                }
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private static boolean fileNotExistsInList(List<File> files, Path path) {
        for (File file : files) {
            if (file.getPath().equals(path.toString())) {
                return false;
            }
        }
        return true;
    }

    public static void validateAndAdd(Path path, String expression, List<File> files) {
        if ((expression == null || Pattern.matches(expression, path.toString())) && fileNotExistsInList(files, path)) {
            files.add(new File(path.toString()));
        }
    }
}
