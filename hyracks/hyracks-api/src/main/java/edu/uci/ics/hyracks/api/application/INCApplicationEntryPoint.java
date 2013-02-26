package edu.uci.ics.hyracks.api.application;

public interface INCApplicationEntryPoint {
    public void start(INCApplicationContext ncAppCtx, String[] args) throws Exception;

    public void notifyStartupComplete() throws Exception;

    public void stop() throws Exception;
}