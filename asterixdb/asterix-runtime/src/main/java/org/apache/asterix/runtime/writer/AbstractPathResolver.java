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
package org.apache.asterix.runtime.writer;

abstract class AbstractPathResolver implements IPathResolver {
    //4-digit format for the partition number, jobId, and file counter
    private static final int PREFIX_NUMBER_OF_DIGITS = 4;
    private static final String PREFIX_FILE_FORMAT = "%0" + PREFIX_NUMBER_OF_DIGITS + "d";
    private static final String FILE_COUNTER_FORMAT = "%0" + getMaxNumberOfDigits() + "d";

    private final String fileExtension;
    private final int fileCounterOffset;
    private final StringBuilder fileStringBuilder;
    protected final char fileSeparator;
    private long fileCounter;

    AbstractPathResolver(String fileExtension, char fileSeparator, int partition) {
        this.fileExtension = fileExtension;
        this.fileSeparator = fileSeparator;
        fileStringBuilder = new StringBuilder();
        fileCounterOffset = initFileBuilder(fileStringBuilder, partition, fileExtension);
        fileCounter = 0;
    }

    @Override
    public final String getNextFileName() {
        fileStringBuilder.setLength(fileCounterOffset);
        fileStringBuilder.append(String.format(FILE_COUNTER_FORMAT, fileCounter++));
        if (fileExtension != null && !fileExtension.isEmpty()) {
            fileStringBuilder.append('.');
            fileStringBuilder.append(fileExtension);
        }
        return fileStringBuilder.toString();
    }

    private static int initFileBuilder(StringBuilder fileStringBuilder, int partition, String fileExtension) {
        fileStringBuilder.append(String.format(PREFIX_FILE_FORMAT, partition));
        fileStringBuilder.append('-');
        int offset = fileStringBuilder.length();
        // dummy
        fileStringBuilder.append(String.format(FILE_COUNTER_FORMAT, 0));
        if (fileExtension != null && !fileExtension.isEmpty()) {
            fileStringBuilder.append('.');
            fileStringBuilder.append(fileExtension);
        }
        return offset;
    }

    private static int getMaxNumberOfDigits() {
        return Long.toString(Long.MAX_VALUE).length();
    }

}
