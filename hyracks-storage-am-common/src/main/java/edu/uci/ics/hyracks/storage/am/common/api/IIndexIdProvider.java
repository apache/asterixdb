package edu.uci.ics.hyracks.storage.am.common.api;

import java.io.Serializable;

public interface IIndexIdProvider extends Serializable {
    public byte[] getIndexId();
}
