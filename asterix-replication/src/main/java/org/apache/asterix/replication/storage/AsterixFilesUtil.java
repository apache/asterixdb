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
package org.apache.asterix.replication.storage;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

public class AsterixFilesUtil {

    public static void deleteFolder(String folderPath) throws IOException {
        File folder = new File(folderPath);
        if (folder.exists()) {
            //delete files inside the folder
            while (deleteDirecotryFiles(folderPath) != true) {
                //if there is a file being written (e.g. LSM Component), wait and try again to delete the file
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    //ignore
                }
            }

            //delete the folder itself
            folder.delete();
        }
    }

    private static boolean deleteDirecotryFiles(String dirPath) throws IOException {
        try {
            Path directory = Paths.get(dirPath);
            Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                    return FileVisitResult.CONTINUE;
                }

            });
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
}
