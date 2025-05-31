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
import java.util.function.Predicate;

import org.apache.asterix.cloud.IWriteBufferProvider;
import org.apache.asterix.cloud.clients.profiler.IRequestProfilerLimiter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.cloud.io.request.ICloudRetryPredicate;
import org.apache.hyracks.control.nc.io.IOManager;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Interface containing methods to perform IO operation on the Cloud Storage
 */
public interface ICloudClient {
    /**
     * @return write buffer size
     */
    int getWriteBufferSize();

    /**
     * @return the requests profiler-limiter
     */
    IRequestProfilerLimiter getProfilerLimiter();

    /**
     * Creates a cloud buffered writer
     *
     * @param bucket         bucket to write to
     * @param path           path to write to
     * @param bufferProvider buffer provider
     * @return cloud writer
     */
    ICloudWriter createWriter(String bucket, String path, IWriteBufferProvider bufferProvider);

    /**
     * Lists objects at the specified bucket and path, and applies the file name filter on the returned objects
     *
     * @param bucket bucket to list from
     * @param path   path to list from
     * @param filter filter to apply
     * @return file names returned after applying the file name filter
     */
    Set<CloudFile> listObjects(String bucket, String path, FilenameFilter filter) throws HyracksDataException;

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
     * @return byte array containing the content, or <code>null</code> if the key does not exist
     * @throws HyracksDataException HyracksDataException
     */
    byte[] readAllBytes(String bucket, String path) throws HyracksDataException;

    /**
     * Returns the {@code InputStream} of an object at the specified bucket and path
     *
     * @param bucket bucket
     * @param path   path
     * @param offset offset
     * @param length length
     * @return input stream of requested range
     */
    InputStream getObjectStream(String bucket, String path, long offset, long length);

    /**
     * Writes the content of the byte array into the bucket at the specified path
     *
     * @param bucket bucket
     * @param path   path
     * @param data   data
     */
    void write(String bucket, String path, byte[] data) throws HyracksDataException;

    /**
     * Copies an object from the source path to the destination path
     *
     * @param bucket   bucket
     * @param srcPath  source path
     * @param destPath destination path
     */
    void copy(String bucket, String srcPath, FileReference destPath) throws HyracksDataException;

    /**
     * Deletes all objects at the specified bucket and paths
     *
     * @param bucket bucket
     * @param paths  paths of all objects to be deleted
     */
    void deleteObjects(String bucket, Collection<String> paths) throws HyracksDataException;

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
    IParallelDownloader createParallelDownloader(String bucket, IOManager ioManager) throws HyracksDataException;

    /**
     * Produces a {@link JsonNode} that contains information about the stored objects in the cloud
     *
     * @param objectMapper to create the result {@link JsonNode}
     * @param bucket       bucket name
     * @return {@link JsonNode} with stored objects' information
     */
    JsonNode listAsJson(ObjectMapper objectMapper, String bucket) throws HyracksDataException;

    /**
     * Performs any necessary closing and cleaning up
     */
    void close() throws HyracksDataException;

    Predicate<Exception> getObjectNotFoundExceptionPredicate();

    default ICloudRetryPredicate getRetryUnlessNotFound() {
        return ex -> Predicate.not(getObjectNotFoundExceptionPredicate()).test(ex);
    }
}
