package edu.uci.ics.hyracks.api.application;

public interface ICCApplicationEntryPoint {
    public void start(ICCApplicationContext ccAppCtx, String[] args) throws Exception;

    public void stop() throws Exception;
}