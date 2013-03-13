package edu.uci.ics.hyracks.hdfs2.scheduler;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.mapred.InputSplit;

/**
 * The wrapped implementation of InputSplit, for the new API scheduler
 * to reuse the old API scheduler
 */
@SuppressWarnings("deprecation")
public class WrappedFileSplit implements InputSplit {

    private String[] locations;
    private long length;

    public WrappedFileSplit(String[] locations, long length) {
        this.locations = locations;
        this.length = length;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        int len = input.readInt();
        locations = new String[len];
        for (int i = 0; i < len; i++)
            locations[i] = input.readUTF();
        length = input.readLong();
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.write(locations.length);
        for (int i = 0; i < locations.length; i++)
            output.writeUTF(locations[i]);
        output.writeLong(length);
    }

    @Override
    public long getLength() throws IOException {
        return length;
    }

    @Override
    public String[] getLocations() throws IOException {
        return locations;
    }

}
