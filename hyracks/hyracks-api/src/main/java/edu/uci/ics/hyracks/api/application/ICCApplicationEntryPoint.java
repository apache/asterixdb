package edu.uci.ics.hyracks.api.application;

public interface ICCApplicationEntryPoint {
    public void appMain(ICCApplicationContext ccAppCtx, String[] args) throws Exception;
}