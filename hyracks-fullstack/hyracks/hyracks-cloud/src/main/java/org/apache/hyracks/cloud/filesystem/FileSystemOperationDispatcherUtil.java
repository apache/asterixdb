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
package org.apache.hyracks.cloud.filesystem;

import java.nio.channels.FileChannel;

import org.apache.hyracks.api.exceptions.HyracksDataException;

public class FileSystemOperationDispatcherUtil {
    private static final IFileSystemOperator FILE_SYSTEM_OPERATOR = createFileSystemOperator();

    public static int getFileDescriptor(FileChannel fileChannel) throws HyracksDataException {
        return FILE_SYSTEM_OPERATOR.getFileDescriptor(fileChannel);
    }

    public static int getBlockSize(int fileDescriptor) throws HyracksDataException {
        return FILE_SYSTEM_OPERATOR.getBlockSize(fileDescriptor);
    }

    public static int punchHole(int fileDescriptor, long offset, long length, int blockSize)
            throws HyracksDataException {
        return (int) FILE_SYSTEM_OPERATOR.punchHole(fileDescriptor, offset, length, blockSize);
    }

    private static IFileSystemOperator createFileSystemOperator() {
        if (isLinux()) {
            return new LinuxFileSystemOperator();
        }

        return new DefaultFileSystemOperator();
    }

    public static String getOSName() {
        return System.getProperty("os.name");
    }

    public static boolean isLinux() {
        String os = getOSName();
        return os.toLowerCase().contains("linux");
    }
}
