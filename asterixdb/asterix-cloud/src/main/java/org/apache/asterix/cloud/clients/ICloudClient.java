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
package org.apache.asterix.cloud.clients;

import java.io.FilenameFilter;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Set;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.control.nc.io.IOManager;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Interface containing methods to perform IO operation on the Cloud Storage
 */
public interface ICloudClient {

    /**
     * Creates a cloud buffered writer
     *
     * @param bucket bucket to write to
     * @param path   path to write to
     * @return buffered writer
     */
    ICloudBufferedWriter createBufferedWriter(String bucket, String path);

    /**
     * Lists objects at the specified bucket and path, and applies the file name filter on the returned objects
     *
     * @param bucket bucket to list from
     * @param path   path to list from
     * @param filter filter to apply
     * @return file names returned after applying the file name filter
     */
    Set<String> listObjects(String bucket, String path, FilenameFilter filter);

    /**
     * Performs a range-read from the specified bucket and path starting at the offset. The amount read is equal to the
     * buffer.remaining()
     *
     * @param bucket bucket
     * @param path   path
     * @param offset offset
     * @param buffer buffer
     * @return returns the buffer position
     */
    int read(String bucket, String path, long offset, ByteBuffer buffer) throws HyracksDataException;

    /**
     * Reads all bytes of an object at the specified bucket and path
     *
     * @param bucket bucket
     * @param path   path
     * @return bytes
     * @throws HyracksDataException HyracksDataException
     */
    byte[] readAllBytes(String bucket, String path) throws HyracksDataException;

    /**
     * Returns the {@code InputStream} of an object at the specified bucket and path
     *
     * @param bucket bucket
     * @param path   path
     * @return inputstream
     */
    InputStream getObjectStream(String bucket, String path);

    /**
     * Writes the content of the byte array into the bucket at the specified path
     *
     * @param bucket bucket
     * @param path   path
     * @param data   data
     */
    void write(String bucket, String path, byte[] data);

    /**
     * Copies an object from the source path to the destination path
     *
     * @param bucket   bucket
     * @param srcPath  source path
     * @param destPath destination path
     */
    void copy(String bucket, String srcPath, FileReference destPath);

    /**
     * Deletes all objects at the specified bucket and paths
     *
     * @param bucket bucket
     * @param paths  paths of all objects to be deleted
     */
    void deleteObjects(String bucket, Collection<String> paths);

    /**
     * Returns the size of the object at the specified path
     *
     * @param bucket bucket
     * @param path   path
     * @return size
     */
    long getObjectSize(String bucket, String path) throws HyracksDataException;

    /**
     * Checks if an object exists at the specified path
     *
     * @param bucket bucket
     * @param path   path
     * @return {@code true} if the object exists, {@code false} otherwise
     */
    boolean exists(String bucket, String path) throws HyracksDataException;

    boolean isEmptyPrefix(String bucket, String path) throws HyracksDataException;

    /**
     * Create a parallel downloader
     *
     * @param bucket    bucket
     * @param ioManager local {@link IOManager}
     * @return an instance of a new parallel downloader
     */
    IParallelDownloader createParallelDownloader(String bucket, IOManager ioManager);

    /**
     * Produces a {@link JsonNode} that contains information about the stored objects in the cloud
     *
     * @param objectMapper to create the result {@link JsonNode}
     * @param bucket       bucket name
     * @return {@link JsonNode} with stored objects' information
     */
    JsonNode listAsJson(ObjectMapper objectMapper, String bucket);

    /**
     * Performs any necessary closing and cleaning up
     */
    void close();
}
