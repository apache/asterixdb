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
package org.apache.asterix.cloud.writer;

import static org.apache.hyracks.api.util.ExceptionUtils.getMessageOrToString;

import org.apache.asterix.cloud.CloudOutputStream;
import org.apache.asterix.cloud.CloudResettableInputStream;
import org.apache.asterix.cloud.IWriteBufferProvider;
import org.apache.asterix.cloud.WriterSingleBufferProvider;
import org.apache.asterix.cloud.clients.ICloudBufferedWriter;
import org.apache.asterix.cloud.clients.ICloudClient;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.runtime.writer.IExternalFilePrinter;
import org.apache.asterix.runtime.writer.IExternalFileWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.data.std.api.IValueReference;

import com.google.common.base.Utf8;

abstract class AbstractCloudExternalFileWriter implements IExternalFileWriter {
    private final IExternalFilePrinter printer;
    private final ICloudClient cloudClient;
    private final String bucket;
    private final boolean partitionedPath;
    private final IWarningCollector warningCollector;
    private final SourceLocation pathSourceLocation;
    private final IWriteBufferProvider bufferProvider;
    private ICloudBufferedWriter bufferedWriter;

    AbstractCloudExternalFileWriter(IExternalFilePrinter printer, ICloudClient cloudClient, String bucket,
            boolean partitionedPath, IWarningCollector warningCollector, SourceLocation pathSourceLocation) {
        this.printer = printer;
        this.cloudClient = cloudClient;
        this.bucket = bucket;
        this.partitionedPath = partitionedPath;
        this.warningCollector = warningCollector;
        this.pathSourceLocation = pathSourceLocation;
        bufferProvider = new WriterSingleBufferProvider();
    }

    @Override
    public final void open() throws HyracksDataException {
        printer.open();
    }

    @Override
    public void validate(String directory) throws HyracksDataException {
        if (checkAndWarnExceedingMaxLength(directory)) {
            return;
        }

        if (partitionedPath && !cloudClient.isEmptyPrefix(bucket, directory)) {
            throw new RuntimeDataException(ErrorCode.DIRECTORY_IS_NOT_EMPTY, pathSourceLocation, directory);
        }
    }

    @Override
    public final boolean newFile(String directory, String fileName) throws HyracksDataException {
        String fullPath = directory + fileName;
        if (checkAndWarnExceedingMaxLength(fullPath)) {
            return false;
        }

        bufferedWriter = cloudClient.createBufferedWriter(bucket, fullPath);
        CloudResettableInputStream inputStream = new CloudResettableInputStream(bufferedWriter, bufferProvider);

        CloudOutputStream outputStream = new CloudOutputStream(inputStream);
        printer.newStream(outputStream);

        return true;
    }

    @Override
    public final void write(IValueReference value) throws HyracksDataException {
        try {
            printer.print(value);
        } catch (HyracksDataException e) {
            throw e;
        } catch (Exception e) {
            if (isSdkException(e)) {
                throw RuntimeDataException.create(ErrorCode.EXTERNAL_SOURCE_ERROR, e, getMessageOrToString(e));
            }
            throw e;
        }
    }

    @Override
    public final void abort() throws HyracksDataException {
        try {
            if (bufferedWriter != null) {
                bufferedWriter.abort();
            }
            printer.close();
        } catch (HyracksDataException e) {
            throw e;
        } catch (Exception e) {
            if (isSdkException(e)) {
                throw RuntimeDataException.create(ErrorCode.EXTERNAL_SOURCE_ERROR, e, getMessageOrToString(e));
            }
            throw e;
        }
    }

    @Override
    public final void close() throws HyracksDataException {
        try {
            printer.close();
        } catch (HyracksDataException e) {
            throw e;
        } catch (Exception e) {
            if (isSdkException(e)) {
                throw RuntimeDataException.create(ErrorCode.EXTERNAL_SOURCE_ERROR, e, getMessageOrToString(e));
            }
            throw e;
        }
    }

    abstract String getAdapterName();

    abstract int getPathMaxLengthInBytes();

    abstract boolean isSdkException(Exception e);

    private boolean checkAndWarnExceedingMaxLength(String fullPath) {
        boolean exceeding = isExceedingMaxLength(fullPath, getPathMaxLengthInBytes());
        if (exceeding && warningCollector.shouldWarn()) {
            warningCollector.warn(Warning.of(pathSourceLocation, ErrorCode.WRITE_PATH_LENGTH_EXCEEDS_MAX_LENGTH,
                    fullPath, getPathMaxLengthInBytes(), getAdapterName()));
        }
        return exceeding;
    }

    static boolean isExceedingMaxLength(String path, int maxLength) {
        return Utf8.encodedLength(path) >= maxLength;
    }
}
