package edu.uci.ics.hyracks.api.application;

public interface INCApplicationEntryPoint {
    public void appMain(INCApplicationContext ncAppCtx, String[] args) throws Exception;
}