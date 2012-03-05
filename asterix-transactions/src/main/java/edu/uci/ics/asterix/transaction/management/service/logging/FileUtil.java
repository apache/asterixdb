/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.transaction.management.service.logging;

import java.io.File;
import java.io.IOException;

/**
 * A utility class that provides operations on files such as creation and
 * loading content in a buffer. It also provides API for creating directories
 */
public class FileUtil {

    public static final String lineSeparator = System.getProperty("line.separator");

    public static boolean createFileIfNotExists(String path) throws IOException {
        File file = new File(path);
        File parentFile = file.getParentFile();
        if (parentFile != null) {
            parentFile.mkdirs();
        }
        return file.createNewFile();
    }

    public static boolean createNewDirectory(String path) throws IOException {
        return (new File(path)).mkdir();
    }

    public static IFileBasedBuffer getFileBasedBuffer(String filePath, long offset, int size) throws IOException {
        IFileBasedBuffer fileBasedBuffer = new FileBasedBuffer(filePath, offset, size);
        return fileBasedBuffer;
    }

}
