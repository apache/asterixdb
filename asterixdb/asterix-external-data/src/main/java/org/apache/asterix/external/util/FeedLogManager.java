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
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TreeSet;

import org.apache.commons.io.FileUtils;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.util.LogRedactionUtil;

public class FeedLogManager implements Closeable {

    public enum LogEntryType {
        START, // partition start
        END, // partition end
        COMMIT, // a record commit within a partition
        SNAPSHOT // an identifier that partitions with identifiers before this one should be ignored
    }

    private static final String PROGRESS_LOG_FILE_NAME = "progress.log";
    private static final String ERROR_LOG_FILE_NAME = "error.log";
    private static final String BAD_RECORDS_FILE_NAME = "failed_record.log";
    private static final String START_PREFIX = "s:";
    private static final String END_PREFIX = "e:";
    private static final String DATE_FORMAT_STRING = "MM/dd/yyyy HH:mm:ss";
    private static final int PREFIX_SIZE = START_PREFIX.length() + DATE_FORMAT_STRING.length() + 1;
    private String currentPartition;
    private final TreeSet<String> completed;
    private final Path dir;
    private BufferedWriter progressLogger;
    private BufferedWriter errorLogger;
    private BufferedWriter recordLogger;
    private final StringBuilder stringBuilder = new StringBuilder();
    private int count = 0;
    private static final DateFormat df = new SimpleDateFormat(DATE_FORMAT_STRING);

    public FeedLogManager(File file) throws HyracksDataException {
        try {
            this.dir = file.toPath();
            this.completed = new TreeSet<>();
            if (!exists()) {
                create();
            }
            open();
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    public synchronized void touch() {
        count++;
    }

    public synchronized void endPartition() throws IOException {
        logProgress(END_PREFIX + currentPartition);
        completed.add(currentPartition);
    }

    public synchronized void endPartition(String partition) throws IOException {
        currentPartition = partition;
        logProgress(END_PREFIX + currentPartition);
        completed.add(currentPartition);
    }

    public synchronized void startPartition(String partition) throws IOException {
        currentPartition = partition;
        logProgress(START_PREFIX + currentPartition);
    }

    public boolean exists() {
        return Files.exists(dir);
    }

    public synchronized void open() throws IOException {
        // read content of logs.
        try (BufferedReader reader = Files.newBufferedReader(
                Paths.get(dir.toAbsolutePath().toString() + File.separator + PROGRESS_LOG_FILE_NAME))) {
            String log = reader.readLine();
            while (log != null) {
                if (log.startsWith(END_PREFIX)) {
                    completed.add(getSplitId(log));
                }
                log = reader.readLine();
            }
        }

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

    @Override
    public synchronized void close() throws IOException {
        count--;
        if (count > 0) {
            return;
        }
        progressLogger.close();
        errorLogger.close();
        recordLogger.close();
    }

    public synchronized boolean create() throws IOException {
        File f = dir.toFile();
        f.mkdirs();
        new File(f, PROGRESS_LOG_FILE_NAME).createNewFile();
        new File(f, ERROR_LOG_FILE_NAME).createNewFile();
        new File(f, BAD_RECORDS_FILE_NAME).createNewFile();
        return true;
    }

    public synchronized boolean destroy() throws IOException {
        File f = dir.toFile();
        FileUtils.deleteDirectory(f);
        return true;
    }

    private synchronized void logProgress(String log) throws IOException {
        stringBuilder.setLength(0);
        stringBuilder.append(df.format((new Date())));
        stringBuilder.append(' ');
        stringBuilder.append(log);
        stringBuilder.append(ExternalDataConstants.LF);
        progressLogger.write(stringBuilder.toString());
        progressLogger.flush();
    }

    public synchronized void logError(String error, Throwable th) throws IOException {
        stringBuilder.setLength(0);
        stringBuilder.append(df.format((new Date())));
        stringBuilder.append(' ');
        stringBuilder.append(error);
        stringBuilder.append(ExternalDataConstants.LF);
        stringBuilder.append(th.toString());
        stringBuilder.append(ExternalDataConstants.LF);
        errorLogger.write(stringBuilder.toString());
        errorLogger.flush();
    }

    public synchronized void logRecord(String record, String errorMessage) throws IOException {
        stringBuilder.setLength(0);
        stringBuilder.append(LogRedactionUtil.userData(record));
        stringBuilder.append(ExternalDataConstants.LF);
        stringBuilder.append(df.format((new Date())));
        stringBuilder.append(' ');
        stringBuilder.append(errorMessage);
        stringBuilder.append(ExternalDataConstants.LF);
        recordLogger.write(stringBuilder.toString());
        recordLogger.flush();
    }

    private static String getSplitId(String log) {
        return log.substring(PREFIX_SIZE);
    }

    public synchronized boolean isSplitRead(String split) {
        return completed.contains(split);
    }
}
