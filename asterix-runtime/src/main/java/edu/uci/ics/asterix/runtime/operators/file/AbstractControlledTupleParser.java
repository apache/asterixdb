package edu.uci.ics.asterix.runtime.operators.file;

import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;

/**
 * An Abstract class implementation for IControlledTupleParser. It provides common
 * functionality involved in parsing data in an external format in a pipelined manner and packing
 * frames with formed tuples.
 * (DONE)
 */
public abstract class AbstractControlledTupleParser extends ControlledTupleParser{

	protected ArrayTupleBuilder tb = new ArrayTupleBuilder(1);
	protected transient DataOutput dos;
	protected final FrameTupleAppender appender;
	protected final ByteBuffer frame;
	protected final ARecordType recType;
	protected final IHyracksTaskContext ctx;
	protected IDataParser parser;
	
	public AbstractControlledTupleParser(IHyracksTaskContext ctx, ARecordType recType) throws HyracksDataException {
		appender = new FrameTupleAppender(ctx.getFrameSize());
		frame = ctx.allocateFrame();
		this.recType = recType;
		this.ctx = ctx;
		dos = tb.getDataOutput();
	}

	public abstract IDataParser getDataParser();

	@Override
	public void parse(InputStream in, IFrameWriter writer)
			throws HyracksDataException {
		//This function when used works as non-pipelined parser
		//This whole parser interface needs revisiting. 
		appender.reset(frame, true);
		parser = getDataParser();
		try {
			parser.initialize(in, recType, true);
			while (true) {
				tb.reset();
				if (!parser.parse(tb.getDataOutput())) {
					parser.reset();
					break;
				}
				tb.addFieldEndOffset();
				addTupleToFrame(writer);
			}
			parser.close();
			if (appender.getTupleCount() > 0) {
				FrameUtils.flushFrame(frame, writer);
			}
		} catch (Exception e) {
			throw new HyracksDataException("Failed to initialize data parser");
		}
	}

	@Override
	public void initialize(InputStream in) throws HyracksDataException {
		appender.reset(frame, true);
		parser = getDataParser();
		try {
			parser.initialize(in, recType, true);

		} catch (Exception e) {
			throw new HyracksDataException("Failed to initialize data parser");
		}
	}

	@Override
	public void parseNext(IFrameWriter writer) throws HyracksDataException {
		try {
			while (true) {
				tb.reset();
				if (!parser.parse(tb.getDataOutput())) {
					parser.reset();
					break;
				}
				tb.addFieldEndOffset();
				addTupleToFrame(writer);
			}
		} catch (AsterixException ae) {
			throw new HyracksDataException(ae);
		} catch (IOException ioe) {
			throw new HyracksDataException(ioe);
		}
	}

	@Override
	public void close(IFrameWriter writer) throws HyracksDataException {
		try{	
			parser.close();
			if (appender.getTupleCount() > 0) {
				FrameUtils.flushFrame(frame, writer);
			}
		} catch (IOException ioe) {
			throw new HyracksDataException(ioe);
		}
	}

	protected void addTupleToFrame(IFrameWriter writer) throws HyracksDataException {
		if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
			FrameUtils.flushFrame(frame, writer);
			appender.reset(frame, true);
			if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
				throw new IllegalStateException();
			}
		}

	}

}