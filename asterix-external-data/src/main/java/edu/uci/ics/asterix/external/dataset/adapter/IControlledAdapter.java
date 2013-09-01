package edu.uci.ics.asterix.external.dataset.adapter;

import java.io.Serializable;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface IControlledAdapter extends Serializable{

	public void initialize(IHyracksTaskContext ctx) throws Exception;
	
	public void processNextFrame(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException;
	
	public void close(IFrameWriter writer) throws HyracksDataException;
}