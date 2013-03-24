package edu.uci.ics.hyracks.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class MiniDFSClusterFactory {

    public MiniDFSCluster getMiniDFSCluster(Configuration conf, int numberOfNC) throws HyracksDataException {
        try {
            return new MiniDFSCluster(conf, numberOfNC, true, null);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }
}
