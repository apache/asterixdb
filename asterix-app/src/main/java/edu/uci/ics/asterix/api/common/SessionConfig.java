package edu.uci.ics.asterix.api.common;

public class SessionConfig {
    private final int port;
    private final boolean optimize;
    private final boolean printExprParam;
    private final boolean printRewrittenExprParam;
    private final boolean printLogicalPlanParam;
    private final boolean printOptimizedLogicalPlanParam;
    private final boolean printPhysicalOpsOnly;
    private final boolean generateJobSpec;
    private final boolean printJob;

    public SessionConfig(int port, boolean optimize, boolean printExprParam, boolean printRewrittenExprParam,
            boolean printLogicalPlanParam, boolean printOptimizedLogicalPlanParam, boolean printPhysicalOpsOnly,
            boolean generateJobSpec, boolean printJob) {
        this.port = port;
        this.optimize = optimize;
        this.printExprParam = printExprParam;
        this.printRewrittenExprParam = printRewrittenExprParam;
        this.printLogicalPlanParam = printLogicalPlanParam;
        this.printOptimizedLogicalPlanParam = printOptimizedLogicalPlanParam;
        this.printPhysicalOpsOnly = printPhysicalOpsOnly;
        this.generateJobSpec = generateJobSpec;
        this.printJob = printJob;
    }

    public int getPort() {
        return port;
    }

    public boolean isPrintExprParam() {
        return printExprParam;
    }

    public boolean isPrintRewrittenExprParam() {
        return printRewrittenExprParam;
    }

    public boolean isPrintLogicalPlanParam() {
        return printLogicalPlanParam;
    }

    public boolean isPrintOptimizedLogicalPlanParam() {
        return printOptimizedLogicalPlanParam;
    }

    public boolean isPrintJob() {
        return printJob;
    }

    public boolean isPrintPhysicalOpsOnly() {
        return printPhysicalOpsOnly;
    }

    public boolean isOptimize() {
        return optimize;
    }

    public boolean isGenerateJobSpec() {
        return generateJobSpec;
    }
}