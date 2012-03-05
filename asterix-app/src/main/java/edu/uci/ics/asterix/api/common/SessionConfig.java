/**
 * 
 */
package edu.uci.ics.asterix.api.common;

public class SessionConfig {
    private int port;
    private boolean printExprParam;
    private boolean printRewrittenExprParam;
    private boolean printLogicalPlanParam;
    private boolean printOptimizedLogicalPlanParam;
    private boolean printPhysicalOpsOnly;
    private boolean printJob;
    private boolean optimize;
    private boolean generateJobSpec = true;

    public SessionConfig(int port, boolean optimize, boolean printExprParam, boolean printRewrittenExprParam,
            boolean printLogicalPlanParam, boolean printOptimizedLogicalPlanParam, boolean printPhysicalOpsOnly,
            boolean printJob) {
        this.setPort(port);
        this.setOptimize(optimize);
        this.setPrintExprParam(printExprParam);
        this.setPrintRewrittenExprParam(printRewrittenExprParam);
        this.setPrintLogicalPlanParam(printLogicalPlanParam);
        this.setPrintOptimizedLogicalPlanParam(printOptimizedLogicalPlanParam);
        this.setPrintPhysicalOpsOnly(printPhysicalOpsOnly);
        this.setPrintJob(printJob);
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getPort() {
        return port;
    }

    public void setPrintExprParam(boolean printExprParam) {
        this.printExprParam = printExprParam;
    }

    public boolean isPrintExprParam() {
        return printExprParam;
    }

    public void setPrintRewrittenExprParam(boolean printRewrittenExprParam) {
        this.printRewrittenExprParam = printRewrittenExprParam;
    }

    public boolean isPrintRewrittenExprParam() {
        return printRewrittenExprParam;
    }

    public void setPrintLogicalPlanParam(boolean printLogicalPlanParam) {
        this.printLogicalPlanParam = printLogicalPlanParam;
    }

    public boolean isPrintLogicalPlanParam() {
        return printLogicalPlanParam;
    }

    public void setPrintOptimizedLogicalPlanParam(boolean printOptimizedLogicalPlanParam) {
        this.printOptimizedLogicalPlanParam = printOptimizedLogicalPlanParam;
    }

    public boolean isPrintOptimizedLogicalPlanParam() {
        return printOptimizedLogicalPlanParam;
    }

    public void setPrintJob(boolean printJob) {
        this.printJob = printJob;
    }

    public boolean isPrintJob() {
        return printJob;
    }

    public void setPrintPhysicalOpsOnly(boolean prinPhysicalOpsOnly) {
        this.printPhysicalOpsOnly = prinPhysicalOpsOnly;
    }

    public boolean isPrintPhysicalOpsOnly() {
        return printPhysicalOpsOnly;
    }

    public void setOptimize(boolean optimize) {
        this.optimize = optimize;
    }

    public boolean isOptimize() {
        return optimize;
    }

    public void setGenerateJobSpec(boolean generateJobSpec) {
        this.generateJobSpec = generateJobSpec;
    }

    public boolean isGenerateJobSpec() {
        return generateJobSpec;
    }
}