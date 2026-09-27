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
package org.apache.asterix.external.input.record.reader.iceberg;

import java.util.Map;

import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

/**
 * Opens every file without the length its manifest records, so that storage is asked for it instead. A table whose
 * manifests record a wrong length cannot be read by length, and this is how a collection reads it anyway.
 * <p>
 * Closing it closes nothing: the FileIO it wraps belongs to the reader, which closes that itself.
 */
final class StorageLengthFileIO implements FileIO {

    private static final long serialVersionUID = 1L;

    private final FileIO delegate;

    StorageLengthFileIO(FileIO delegate) {
        this.delegate = delegate;
    }

    @Override
    public InputFile newInputFile(String path) {
        return delegate.newInputFile(path);
    }

    @Override
    public InputFile newInputFile(String path, long length) {
        return delegate.newInputFile(path);
    }

    @Override
    public OutputFile newOutputFile(String path) {
        return delegate.newOutputFile(path);
    }

    @Override
    public void deleteFile(String path) {
        delegate.deleteFile(path);
    }

    @Override
    public Map<String, String> properties() {
        return delegate.properties();
    }
}
