package edu.uci.ics.asterix.common.config;

import edu.uci.ics.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig; 

public class OptimizationConfUtil {

    private static final PhysicalOptimizationConfig physicalOptimizationConfig = new PhysicalOptimizationConfig();

    public static PhysicalOptimizationConfig getPhysicalOptimizationConfig() {
        return physicalOptimizationConfig;
    }
}
