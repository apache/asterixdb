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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.TreeSet;

import org.apache.commons.io.FileUtils;

public class FeedLogManager {

    public enum LogEntryType {
        START,      // partition start
        END,        // partition end
        COMMIT,     // a record commit within a partition
        SNAPSHOT    // an identifier that partitions with identifiers before this one should be ignored
    }

    public static final String PROGRESS_LOG_FILE_NAME = "progress.log";
    public static final String ERROR_LOG_FILE_NAME = "error.log";
    public static final String BAD_RECORDS_FILE_NAME = "failed_record.log";
    public static final String START_PREFIX = "s:";
    public static final String END_PREFIX = "e:";
    public static final int PREFIX_SIZE = 2;
    private String currentPartition;
    private TreeSet<String> completed;
    private Path dir;
    private BufferedWriter progressLogger;
    private BufferedWriter errorLogger;
    private BufferedWriter recordLogger;

    public FeedLogManager(File file) {
        this.dir = file.toPath();
        this.completed = new TreeSet<String>();
    }

    public void endPartition() throws IOException {
        logProgress(END_PREFIX + currentPartition);
        completed.add(currentPartition);
    }

    public void endPartition(String partition) throws IOException {
        currentPartition = partition;
        logProgress(END_PREFIX + currentPartition);
        completed.add(currentPartition);
    }

    public void startPartition(String partition) throws IOException {
        currentPartition = partition;
        logProgress(START_PREFIX + currentPartition);
    }

    public boolean exists() {
        return Files.exists(dir);
    }

    public void open() throws IOException {
        // read content of logs.
        BufferedReader reader = Files.newBufferedReader(
                Paths.get(dir.toAbsolutePath().toString() + File.separator + PROGRESS_LOG_FILE_NAME));
        String log = reader.readLine();
        while (log != null) {
            if (log.startsWith(END_PREFIX)) {
                completed.add(getSplitId(log));
            }
            log = reader.readLine();
        }
        reader.close();

        progressLogger = Files.newBufferedWriter(
                Paths.get(dir.toAbsolutePath().toString() + File.separator + PROGRESS_LOG_FILE_NAME),
                StandardCharsets.UTF_8, StandardOpenOption.APPEND);
        errorLogger = Files.newBufferedWriter(
                Paths.get(dir.toAbsolutePath().toString() + File.separator + ERROR_LOG_FILE_NAME),
                StandardCharsets.UTF_8, StandardOpenOption.APPEND);
        recordLogger = Files.newBufferedWriter(
                Paths.get(dir.toAbsolutePath().toString() + File.separator + BAD_RECORDS_FILE_NAME),
                StandardCharsets.UTF_8, StandardOpenOption.APPEND);
    }

    public void close() throws IOException {
        progressLogger.close();
        errorLogger.close();
        recordLogger.close();
    }

    public boolean create() throws IOException {
        File f = dir.toFile();
        f.mkdirs();
        new File(f, PROGRESS_LOG_FILE_NAME).createNewFile();
        new File(f, ERROR_LOG_FILE_NAME).createNewFile();
        new File(f, BAD_RECORDS_FILE_NAME).createNewFile();
        return true;
    }

    public boolean destroy() throws IOException {
        File f = dir.toFile();
        FileUtils.deleteDirectory(f);
        return true;
    }

    public void logProgress(String log) throws IOException {
        progressLogger.write(log);
        progressLogger.newLine();
    }

    public void logError(String error, Throwable th) throws IOException {
        errorLogger.append(error);
        errorLogger.newLine();
        errorLogger.append(th.toString());
        errorLogger.newLine();
    }

    public void logRecord(String record, Exception e) throws IOException {
        recordLogger.append(record);
        recordLogger.newLine();
        recordLogger.append(e.toString());
        recordLogger.newLine();
    }

    public static String getSplitId(String log) {
        return log.substring(PREFIX_SIZE);
    }

    public boolean isSplitRead(String split) {
        return completed.contains(split);
    }
}
