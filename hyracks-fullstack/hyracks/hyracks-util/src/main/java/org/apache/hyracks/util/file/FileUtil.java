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
package org.apache.hyracks.util.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FileUtil {

    private static final Logger LOGGER = LogManager.getLogger();
    private static final Pattern PARENT_DIR = Pattern.compile("([/\\\\]|^)[^./\\\\]+[/\\\\]\\.\\.([/\\\\]|$)");
    private static final Object LOCK = new Object();
    private static final int MAX_COPY_ATTEMPTS = 3;

    private FileUtil() {
    }

    public static String joinPath(String... elements) {
        return joinPath(File.separatorChar, elements);
    }

    public static String joinPathUnix(String... elements) {
        return joinPath('/', elements);
    }

    public static String joinPathWindows(String... elements) {
        return joinPath('\\', elements);
    }

    public static void forceMkdirs(File dir) throws IOException {
        File canonicalDir = dir.getCanonicalFile();
        try {
            FileUtils.forceMkdir(canonicalDir);
        } catch (IOException e) {
            LOGGER.warn("failure to create directory {}, retrying", dir, e);
            synchronized (LOCK) {
                FileUtils.forceMkdir(canonicalDir);
            }
        }

    }

    static String joinPath(char separatorChar, String... elements) {
        final String separator = String.valueOf(separatorChar);
        final String escapedSeparator = Pattern.quote(separator);
        String joined = String.join(separator, elements);
        if (separatorChar == '\\') {
            // preserve leading double-slash on windows (UNC)
            return joined.replaceAll("([^" + escapedSeparator + "])(" + escapedSeparator + ")+", "$1$2")
                    .replaceAll(escapedSeparator + "$", "");
        } else {
            return joined.replaceAll("(" + escapedSeparator + ")+", "$1").replaceAll(escapedSeparator + "$", "");
        }
    }

    public static void safeCopyFile(File child, File destChild) throws IOException {
        forceMkdirs(destChild.getParentFile());
        IOException ioException = null;
        while (true) {
            try {
                FileUtils.copyFile(child, destChild);
                return;
            } catch (IOException e) {
                if (ioException == null) {
                    ioException = e;
                } else {
                    ioException.addSuppressed(e);
                }
                if (ioException.getSuppressed().length >= MAX_COPY_ATTEMPTS) {
                    LOGGER.warn("Unable to copy {} to {} after " + MAX_COPY_ATTEMPTS + " attempts; skipping file",
                            child, destChild, e);
                    return;
                }
            }
        }
    }

    public static void writeAndForce(Path path, byte[] data) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw");) {
            raf.write(data);
            raf.getChannel().force(true);
        }
    }

    public static String canonicalize(CharSequence path) {
        String newPath = path.toString();
        Matcher matcher = PARENT_DIR.matcher(newPath);
        while (matcher.find()) {
            // TODO(mblow): use StringBuilder once Java 8 is no longer supported (requires >=9)
            StringBuffer sb = new StringBuffer();
            matcher.appendReplacement(sb, matcher.group(2).isEmpty() ? "" : matcher.group(1).replace("\\", "\\\\"));
            matcher.appendTail(sb);
            newPath = sb.toString();
            matcher.reset(newPath);
        }
        return newPath;
    }

    public static File canonicalize(File file) {
        return new File(canonicalize(file.getPath()));
    }
}
