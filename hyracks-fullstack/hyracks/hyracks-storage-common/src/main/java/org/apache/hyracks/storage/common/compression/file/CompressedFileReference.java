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
package org.apache.hyracks.storage.common.compression.file;

import java.util.Objects;

import org.apache.hyracks.api.compression.ICompressorDecompressor;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.IODeviceHandle;

public class CompressedFileReference extends FileReference {
    private static final long serialVersionUID = 1L;

    private final String lafPath;
    private final FileReference lafFileRef;
    private final transient ICompressorDecompressor compressorDecompressor;

    public CompressedFileReference(IODeviceHandle dev, ICompressorDecompressor compressorDecompressor, String path,
            String lafPath) {
        super(dev, path);
        this.lafPath = lafPath;
        lafFileRef = new FileReference(dev, lafPath);
        this.compressorDecompressor = compressorDecompressor;
    }

    public FileReference getLAFFileReference() {
        return lafFileRef;
    }

    public ICompressorDecompressor getCompressorDecompressor() {
        return compressorDecompressor;
    }

    @Override
    public boolean delete() {
        return lafFileRef.delete() && super.delete();
    }

    @Override
    public boolean isCompressed() {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof CompressedFileReference)) {
            return false;
        }
        return super.equals(o) && lafPath.equals(((CompressedFileReference) o).lafPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.getRelativePath(), lafPath);
    }

    /**
     * @return the relative path for LAF file
     */
    public String getLAFRelativePath() {
        return lafPath;
    }

    /**
     * @return the absolute path for LAF file
     */
    public String getLAFAbsolutePath() {
        return lafFileRef.getAbsolutePath();
    }
}
