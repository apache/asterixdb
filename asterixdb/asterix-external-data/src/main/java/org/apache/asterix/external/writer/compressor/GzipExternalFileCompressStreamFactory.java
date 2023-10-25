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
package org.apache.asterix.external.writer.compressor;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class GzipExternalFileCompressStreamFactory implements IExternalFileCompressStreamFactory {
    private static final long serialVersionUID = -7364595253362922025L;
    public static IExternalFileCompressStreamFactory INSTANCE = new GzipExternalFileCompressStreamFactory();

    private GzipExternalFileCompressStreamFactory() {
    }

    @Override
    public OutputStream createStream(OutputStream outputStream) throws HyracksDataException {
        try {
            return new GzipCompressorOutputStream(outputStream);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }
}
