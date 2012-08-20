package edu.uci.ics.hyracks.storage.am.common.api;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface ICloseableResource {
    public void close() throws HyracksDataException;
}
