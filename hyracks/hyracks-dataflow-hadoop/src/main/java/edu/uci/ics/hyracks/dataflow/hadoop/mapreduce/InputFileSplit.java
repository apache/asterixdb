package edu.uci.ics.hyracks.dataflow.hadoop.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InputFileSplit extends InputSplit implements Writable {
    private Path file;
    private long start;
    private long length;
    private int blockId;
    private String[] hosts;
    private long scheduleTime;

    public InputFileSplit() {
    }

    public InputFileSplit(int blockId, Path file, long start, long length, String[] hosts, long schedule_time) {
        this.blockId = blockId;
        this.file = file;
        this.start = start;
        this.length = length;
        this.hosts = hosts;
        this.scheduleTime = schedule_time;
    }

    public int blockId() {
        return blockId;
    }

    public long scheduleTime() {
        return this.scheduleTime;
    }

    public Path getPath() {
        return file;
    }

    /** The position of the first byte in the file to process. */
    public long getStart() {
        return start;
    }

    /** The number of bytes in the file to process. */
    @Override
    public long getLength() {
        return length;
    }

    @Override
    public String toString() {
        return file + ":" + start + "+" + length;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, file.toString());
        out.writeLong(start);
        out.writeLong(length);
        out.writeInt(blockId);
        out.writeLong(this.scheduleTime);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        file = new Path(Text.readString(in));
        start = in.readLong();
        length = in.readLong();
        hosts = null;
        this.blockId = in.readInt();
        this.scheduleTime = in.readLong();
    }

    @Override
    public String[] getLocations() throws IOException {
        if (this.hosts == null) {
            return new String[] {};
        } else {
            return this.hosts;
        }
    }

    public FileSplit toFileSplit() {
        return new FileSplit(file, start, length, hosts);
    }
}