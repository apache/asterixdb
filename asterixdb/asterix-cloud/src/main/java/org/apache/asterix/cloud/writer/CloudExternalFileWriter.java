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

import org.apache.asterix.cloud.CloudOutputStream;
import org.apache.asterix.cloud.CloudResettableInputStream;
import org.apache.asterix.cloud.IWriteBufferProvider;
import org.apache.asterix.cloud.WriterSingleBufferProvider;
import org.apache.asterix.cloud.clients.ICloudBufferedWriter;
import org.apache.asterix.cloud.clients.ICloudClient;
import org.apache.asterix.runtime.writer.IExternalFilePrinter;
import org.apache.asterix.runtime.writer.IExternalFileWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;

final class CloudExternalFileWriter implements IExternalFileWriter {
    private final IExternalFilePrinter printer;
    private final ICloudClient cloudClient;
    private final String bucket;
    private final IWriteBufferProvider bufferProvider;
    private ICloudBufferedWriter bufferedWriter;

    public CloudExternalFileWriter(IExternalFilePrinter printer, ICloudClient cloudClient, String bucket) {
        this.printer = printer;
        this.cloudClient = cloudClient;
        this.bucket = bucket;
        bufferProvider = new WriterSingleBufferProvider();
    }

    @Override
    public void open() throws HyracksDataException {
        printer.open();
    }

    @Override
    public void newFile(String path) throws HyracksDataException {
        bufferedWriter = cloudClient.createBufferedWriter(bucket, path);
        CloudResettableInputStream inputStream = new CloudResettableInputStream(bufferedWriter, bufferProvider);

        CloudOutputStream outputStream = new CloudOutputStream(inputStream);
        printer.newStream(outputStream);
    }

    @Override
    public void write(IValueReference value) throws HyracksDataException {
        printer.print(value);
    }

    @Override
    public void abort() throws HyracksDataException {
        bufferedWriter.abort();
        printer.close();
    }

    @Override
    public void close() throws HyracksDataException {
        printer.close();
    }
}
