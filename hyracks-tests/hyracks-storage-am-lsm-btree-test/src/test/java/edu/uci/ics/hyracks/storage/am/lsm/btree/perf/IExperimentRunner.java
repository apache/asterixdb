package edu.uci.ics.hyracks.storage.am.lsm.btree.perf;

import edu.uci.ics.hyracks.storage.am.common.datagen.DataGenThread;

public interface IExperimentRunner {
    public static int DEFAULT_MAX_OUTSTANDING = 100000;
    
    public void init() throws Exception;
    
    public long runExperiment(DataGenThread dataGen, int numThreads) throws Exception;
    
    public void reset() throws Exception;
    
    public void deinit() throws Exception;
}
