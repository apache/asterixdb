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

import java.io.FileDescriptor;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;

import org.apache.hyracks.api.exceptions.HyracksDataException;

import jnr.ffi.LibraryLoader;
import jnr.posix.FileStat;
import jnr.posix.POSIX;
import jnr.posix.POSIXFactory;

class LinuxFileSystemOperator implements IFileSystemOperator {
    /**
     * Load native library
     */
    private static final LinuxNativeLibC libc = LibraryLoader.create(LinuxNativeLibC.class).failImmediately().load("c");
    /**
     * Load POSIX
     */
    private static final POSIX posix = POSIXFactory.getPOSIX();

    /**
     * default is extend size
     */
    public static final int FALLOC_FL_KEEP_SIZE = 0x01;

    /**
     * de-allocates range
     */
    public static final int FALLOC_FL_PUNCH_HOLE = 0x02;

    @Override
    public int getFileDescriptor(FileChannel fileChannel) throws HyracksDataException {
        FileDescriptor fd = getField(fileChannel, "fd", FileDescriptor.class);
        return getField(fd, "fd", int.class);
    }

    @Override
    public int getBlockSize(int fileDescriptor) throws HyracksDataException {
        FileStat stat = posix.fstat(fileDescriptor);
        return Math.toIntExact(stat.blockSize());
    }

    /**
     * @param fileDescriptor of the file
     * @param offset         starting offset
     * @param length         length
     * @param blockSize      block size
     * @return length of the hole
     * @see <a href="https://github.com/apache/ignite/tree/master/modules/compress/src/main/java/org/apache/ignite/internal/processors/compress">Apache Ignite Internal</a>
     */
    @Override
    public long punchHole(int fileDescriptor, long offset, long length, int blockSize) throws HyracksDataException {
        assert offset >= 0;
        assert length > 0;

        if (length < blockSize) {
            return 0;
        }

        long off = offset;
        long len = length;
        // TODO maybe optimize for power of 2
        if (off % blockSize != 0) {
            long end = off + len;
            off = (off / blockSize + 1) * blockSize;
            len = end - off;

            if (len <= 0) {
                return 0;
            }
        }

        len = len / blockSize * blockSize;

        if (len > 0) {
            int res = libc.fallocate(fileDescriptor, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, off, len);
            if (res != 0) {
                throw new HyracksDataException("error");
            }
        }

        return len;
    }

    private <T> T getField(Object object, String name, Class<T> clazz) throws HyracksDataException {
        try {
            Field field = object.getClass().getDeclaredField(name);
            field.setAccessible(true);
            return clazz.cast(field.get(field));
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw HyracksDataException.create(e);
        }
    }

    public interface LinuxNativeLibC {
        /**
         * Allows the caller to directly manipulate the allocated
         * disk space for the file referred to by fd for the byte range starting
         * at {@code off} offset and continuing for {@code len} bytes.
         *
         * @param fd   file descriptor.
         * @param mode determines the operation to be performed on the given range.
         * @param off  required position offset.
         * @param len  required length.
         * @return On success, fallocate() returns zero.  On error, -1 is returned and
         * {@code errno} is set to indicate the error.
         */
        int fallocate(int fd, int mode, long off, long len);
    }
}
