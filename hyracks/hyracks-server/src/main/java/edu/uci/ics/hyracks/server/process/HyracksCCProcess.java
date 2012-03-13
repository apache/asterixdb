package edu.uci.ics.hyracks.server.process;

import java.util.List;

import edu.uci.ics.hyracks.control.cc.CCDriver;
import edu.uci.ics.hyracks.control.common.controllers.CCConfig;

public class HyracksCCProcess extends HyracksServerProcess {
    private CCConfig config;

    public HyracksCCProcess(CCConfig config) {
        this.config = config;
    }

    @Override
    protected void addCmdLineArgs(List<String> cList) {
        config.toCommandLine(cList);
    }

    @Override
    protected String getMainClassName() {
        return CCDriver.class.getName();
    }
}