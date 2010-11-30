package edu.uci.ics.hyracks.dataflow.std.sort;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class RunFileReader implements IFrameReader {
    private final File file;
    private FileChannel channel;

    public RunFileReader(File file) throws FileNotFoundException {
        this.file = file;
    }

    @Override
    public void open() throws HyracksDataException {
        RandomAccessFile raf;
        try {
            raf = new RandomAccessFile(file, "r");
        } catch (FileNotFoundException e) {
            throw new HyracksDataException(e);
        }
        channel = raf.getChannel();
    }

    @Override
    public boolean nextFrame(ByteBuffer buffer) throws HyracksDataException {
        buffer.clear();
        int remain = buffer.capacity();
        while (remain > 0) {
            int len;
            try {
                len = channel.read(buffer);
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
            if (len < 0) {
                return false;
            }
            remain -= len;
        }
        return true;
    }

    @Override
    public void close() throws HyracksDataException {
        try {
            channel.close();
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }
}