package edu.uci.ics.asterix.runtime.operators.file;

import java.io.InputStream;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParser;

/**
 * This interface is to be implemented by parsers used in a pipelined hyracks job where input is not ready all at once
 */
public abstract class ControlledTupleParser implements ITupleParser{

	/**
	 * This function associate an input stream with the parser
	 */
	public abstract void initialize(InputStream in) throws HyracksDataException;
	
	/**
	 * This function should flush the tuples setting in the frame writer buffer
	 * and free all resources
	 */
	public abstract void close(IFrameWriter writer) throws HyracksDataException;

	/**
	 * This function is called when there are more data ready for parsing in the input stream
	 */
	public abstract void parseNext(IFrameWriter writer) throws HyracksDataException;
}