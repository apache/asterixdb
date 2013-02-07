package edu.uci.ics.hyracks.hdfs.dataflow;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.Serializable;

import org.apache.hadoop.mapred.JobConf;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

@SuppressWarnings("deprecation")
public class ConfFactory implements Serializable {
    private static final long serialVersionUID = 1L;
    private byte[] confBytes;

    public ConfFactory(JobConf conf) throws HyracksDataException {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);
            conf.write(dos);
            confBytes = bos.toByteArray();
            dos.close();
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    public JobConf getConf() throws HyracksDataException {
        try {
            JobConf conf = new JobConf();
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(confBytes));
            conf.readFields(dis);
            dis.close();
            return conf;
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }
}
