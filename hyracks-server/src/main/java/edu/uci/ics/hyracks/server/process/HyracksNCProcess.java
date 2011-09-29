package edu.uci.ics.hyracks.server.process;

import java.util.List;

import edu.uci.ics.hyracks.control.common.controllers.NCConfig;
import edu.uci.ics.hyracks.control.nc.NCDriver;

public class HyracksNCProcess extends HyracksServerProcess {
    private NCConfig config;

    public HyracksNCProcess(NCConfig config) {
        this.config = config;
    }

    @Override
    protected void addCmdLineArgs(List<String> cList) {
        config.toCommandLine(cList);
    }

    @Override
    protected String getMainClassName() {
        return NCDriver.class.getName();
    }
}