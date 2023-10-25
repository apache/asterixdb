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

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

abstract class AbstractPathResolver implements IPathResolver {
    // TODO is 4-digits enough?
    // TODO do we need jobId?
    //4-digit format for the partition number, jobId, and file counter
    private static final int NUMBER_OF_DIGITS = 4;
    private static final String FILE_FORMAT = "%0" + NUMBER_OF_DIGITS + "d";

    private final String fileExtension;
    private final char fileSeparator;
    private final int partition;
    private final long jobId;
    private final StringBuilder pathStringBuilder;
    private int fileCounter;

    AbstractPathResolver(String fileExtension, char fileSeparator, int partition, long jobId) {
        this.fileExtension = fileExtension;
        this.fileSeparator = fileSeparator;
        this.partition = partition;
        this.jobId = jobId;
        pathStringBuilder = new StringBuilder();
        fileCounter = 0;
    }

    @Override
    public final String getPartitionPath(IFrameTupleReference tuple) throws HyracksDataException {
        fileCounter = 0;
        pathStringBuilder.setLength(0);
        appendPrefix(pathStringBuilder, tuple);
        if (pathStringBuilder.charAt(pathStringBuilder.length() - 1) != fileSeparator) {
            pathStringBuilder.append(fileSeparator);
        }
        pathStringBuilder.append(String.format(FILE_FORMAT, partition));
        pathStringBuilder.append('-');
        pathStringBuilder.append(String.format(FILE_FORMAT, jobId));
        pathStringBuilder.append('-');
        pathStringBuilder.append(String.format(FILE_FORMAT, fileCounter++));
        if (fileExtension != null && !fileExtension.isEmpty()) {
            pathStringBuilder.append('.');
            pathStringBuilder.append(fileExtension);
        }
        return pathStringBuilder.toString();
    }

    @Override
    public final String getNextPath() {
        int numOfCharToRemove = NUMBER_OF_DIGITS;
        if (fileExtension != null && !fileExtension.isEmpty()) {
            numOfCharToRemove += 1 + fileExtension.length();
        }
        pathStringBuilder.setLength(pathStringBuilder.length() - numOfCharToRemove);
        pathStringBuilder.append(String.format(FILE_FORMAT, fileCounter++));
        if (fileExtension != null && !fileExtension.isEmpty()) {
            pathStringBuilder.append('.');
            pathStringBuilder.append(fileExtension);
        }
        return pathStringBuilder.toString();
    }

    abstract void appendPrefix(StringBuilder pathStringBuilder, IFrameTupleReference tuple) throws HyracksDataException;
}
