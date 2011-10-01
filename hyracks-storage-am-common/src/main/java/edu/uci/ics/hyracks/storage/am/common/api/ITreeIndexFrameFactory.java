package edu.uci.ics.hyracks.storage.am.common.api;

import java.io.Serializable;

public interface ITreeIndexFrameFactory extends Serializable {
    public ITreeIndexFrame createFrame();
    public ITreeIndexTupleWriterFactory getTupleWriterFactory();
}
