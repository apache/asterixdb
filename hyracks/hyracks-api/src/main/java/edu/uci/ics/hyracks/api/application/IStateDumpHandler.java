package edu.uci.ics.hyracks.api.application;

import java.io.IOException;
import java.io.OutputStream;

public interface IStateDumpHandler {
    public void dumpState(OutputStream os) throws IOException;
}
