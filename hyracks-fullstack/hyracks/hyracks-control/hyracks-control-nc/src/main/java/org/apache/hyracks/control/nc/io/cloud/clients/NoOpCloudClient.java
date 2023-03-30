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
package org.apache.hyracks.control.nc.io.cloud.clients;

import java.io.FilenameFilter;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Set;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;

public class NoOpCloudClient implements ICloudClient {

    public static final NoOpCloudClient INSTANCE = new NoOpCloudClient();

    private NoOpCloudClient() {
        // do not instantiate
    }

    @Override
    public ICloudBufferedWriter createBufferedWriter(String bucket, String path) {
        return null;
    }

    @Override
    public Set<String> listObjects(String bucket, String path, FilenameFilter filter) {
        return null;
    }

    @Override
    public int read(String bucket, String path, long offset, ByteBuffer buffer) throws HyracksDataException {
        return 0;
    }

    @Override
    public byte[] readAllBytes(String bucket, String path) throws HyracksDataException {
        return new byte[0];
    }

    @Override
    public InputStream getObjectStream(String bucket, String path) {
        return null;
    }

    @Override
    public void write(String bucket, String path, byte[] data) {

    }

    @Override
    public void copy(String bucket, String srcPath, FileReference destPath) {

    }

    @Override
    public void deleteObject(String bucket, String path) {

    }

    @Override
    public long getObjectSize(String bucket, String path) {
        return 0;
    }

    @Override
    public boolean exists(String bucket, String path) {
        return false;
    }
}