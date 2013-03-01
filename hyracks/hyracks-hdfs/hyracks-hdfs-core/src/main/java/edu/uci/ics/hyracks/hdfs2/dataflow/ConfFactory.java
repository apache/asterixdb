package edu.uci.ics.hyracks.hdfs2.dataflow;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.Serializable;

import org.apache.hadoop.mapreduce.Job;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class ConfFactory implements Serializable {
    private static final long serialVersionUID = 1L;
    private byte[] confBytes;

    public ConfFactory(Job conf) throws HyracksDataException {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);
            conf.getConfiguration().write(dos);
            confBytes = bos.toByteArray();
            dos.close();
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    public Job getConf() throws HyracksDataException {
        try {
            Job conf = new Job();
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(confBytes));
            conf.getConfiguration().readFields(dis);
            dis.close();
            return conf;
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }
}
