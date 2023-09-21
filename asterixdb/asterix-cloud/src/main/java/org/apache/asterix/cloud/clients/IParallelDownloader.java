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

import java.util.Collection;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;

public interface IParallelDownloader {

    /**
     * Downloads files in all partitions
     *
     * @param toDownload all files to be downloaded
     */
    void downloadFiles(Collection<FileReference> toDownload) throws HyracksDataException;

    /**
     * Downloads files in all partitions
     *
     * @param toDownload all files to be downloaded
     * @return file that failed to download
     */
    Collection<FileReference> downloadDirectories(Collection<FileReference> toDownload) throws HyracksDataException;

    /**
     * Close the downloader and release all of its resources
     */
    void close();
}
