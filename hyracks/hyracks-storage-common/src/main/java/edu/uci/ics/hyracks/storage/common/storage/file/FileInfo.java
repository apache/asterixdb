package edu.uci.ics.hyracks.storage.common.storage.file;

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