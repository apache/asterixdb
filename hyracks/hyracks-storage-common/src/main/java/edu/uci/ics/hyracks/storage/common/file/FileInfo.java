/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.storage.common.file;

import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public class FileInfo {
    private final int fileId;
    private final RandomAccessFile file;
    private FileChannel channel;

    public FileInfo(int fileId, RandomAccessFile file) {
        this.fileId = fileId;
        this.file = file;
        channel = file.getChannel();
    }

    public int getFileId() {
        return fileId;
    }

    public RandomAccessFile getFile() {
        return file;
    }

    public FileChannel getFileChannel() {
        return channel;
    }

    public long getDiskPageId(int pageId) {
        return getDiskPageId(fileId, pageId);
    }

    public static long getDiskPageId(int fileId, int pageId) {
        return (((long) fileId) << 32) + pageId;
    }

    public static int getFileId(long dpid) {
        return (int) ((dpid >> 32) & 0xffffffff);
    }

    public static int getPageId(long dpid) {
        return (int) (dpid & 0xffffffff);
    }
}