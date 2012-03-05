package edu.uci.ics.asterix.runtime.operators.file;

import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.util.Arrays;

import edu.uci.ics.asterix.builders.IARecordBuilder;
import edu.uci.ics.asterix.builders.RecordBuilder;
import edu.uci.ics.asterix.om.base.AMutableString;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParser;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;

public class DelimitedDataTupleParser extends AbstractTupleParser {

	protected final IValueParserFactory[] valueParserFactories;
	protected final char fieldDelimiter;
	protected final IHyracksTaskContext ctx;
	protected final ARecordType recType;
	protected ArrayTupleBuilder tb = new ArrayTupleBuilder(1);
	protected DataOutput recDos = tb.getDataOutput();
	protected final FrameTupleAppender appender;
	protected final ByteBuffer frame;
	

	public DelimitedDataTupleParser(IHyracksTaskContext ctx,
			ARecordType recType, IValueParserFactory[] valueParserFactories,
			char fieldDelimter) {
		this.valueParserFactories = valueParserFactories;
		this.fieldDelimiter = fieldDelimter;
		this.ctx = ctx;
		this.recType = recType;
		appender = new FrameTupleAppender(ctx.getFrameSize());
		frame = ctx.allocateFrame();
	}

	@Override
	public void parse(InputStream in, IFrameWriter writer)
			throws HyracksDataException {
		try {
			IValueParser[] valueParsers = new IValueParser[valueParserFactories.length];
			for (int i = 0; i < valueParserFactories.length; ++i) {
				valueParsers[i] = valueParserFactories[i].createValueParser();
			}
		
			appender.reset(frame, true);
			

			ArrayBackedValueStorage fieldValueBuffer = new ArrayBackedValueStorage();
			DataOutput fieldValueBufferOutput = fieldValueBuffer
					.getDataOutput();
			IARecordBuilder recBuilder = new RecordBuilder();
			recBuilder.reset(recType);
			recBuilder.init();

			int n = recType.getFieldNames().length;
			byte[] fieldTypeTags = new byte[n];
			for (int i = 0; i < n; i++) {
				ATypeTag tag = recType.getFieldTypes()[i].getTypeTag();
				fieldTypeTags[i] = tag.serialize();
			}

			int[] fldIds = new int[n];
			ArrayBackedValueStorage[] nameBuffers = new ArrayBackedValueStorage[n];
			AMutableString str = new AMutableString(null);
			for (int i = 0; i < n; i++) {
				String name = recType.getFieldNames()[i];
				fldIds[i] = recBuilder.getFieldId(name);
				if (fldIds[i] < 0) {
					if (!recType.isOpen()) {
						throw new HyracksDataException("Illegal field " + name
								+ " in closed type " + recType);
					} else {
						nameBuffers[i] = new ArrayBackedValueStorage();
						fieldNameToBytes(name, str, nameBuffers[i]);
					}
				}
			}

			FieldCursor cursor = new FieldCursor(new InputStreamReader(in));
			while (cursor.nextRecord()) {
				tb.reset();
				recBuilder.reset(recType);
				recBuilder.init();

				for (int i = 0; i < valueParsers.length; ++i) {
					if (!cursor.nextField()) {
						break;
					}
					fieldValueBuffer.reset();
					fieldValueBufferOutput.writeByte(fieldTypeTags[i]);
					valueParsers[i]
							.parse(cursor.buffer, cursor.fStart, cursor.fEnd
									- cursor.fStart, fieldValueBufferOutput);
					if (fldIds[i] < 0) {
						recBuilder.addField(nameBuffers[i], fieldValueBuffer);
					} else {
						recBuilder.addField(fldIds[i], fieldValueBuffer);
					}
				}
				recBuilder.write(recDos, true);
				tb.addFieldEndOffset();

				if (!appender.append(tb.getFieldEndOffsets(),
						tb.getByteArray(), 0, tb.getSize())) {
					FrameUtils.flushFrame(frame, writer);
					appender.reset(frame, true);
					if (!appender.append(tb.getFieldEndOffsets(),
							tb.getByteArray(), 0, tb.getSize())) {
						throw new IllegalStateException();
					}
				}
			}
			if (appender.getTupleCount() > 0) {
				FrameUtils.flushFrame(frame, writer);
			}
		} catch (IOException e) {
			throw new HyracksDataException(e);
		}
	}

	protected void fieldNameToBytes(String fieldName, AMutableString str,
			ArrayBackedValueStorage buffer) throws HyracksDataException {
		buffer.reset();
		DataOutput out = buffer.getDataOutput();
		str.setValue(fieldName);
		try {
			stringSerde.serialize(str, out);
		} catch (IOException e) {
			throw new HyracksDataException(e);
		}
	}

	protected enum State {
		INIT, IN_RECORD, EOR, CR, EOF
	}

	protected class FieldCursor {
		private static final int INITIAL_BUFFER_SIZE = 4096;
		private static final int INCREMENT = 4096;

		private final Reader in;

		private char[] buffer;
		private int start;
		private int end;
		private State state;

		private int fStart;
		private int fEnd;

		public FieldCursor(Reader in) {
			this.in = in;
			buffer = new char[INITIAL_BUFFER_SIZE];
			start = 0;
			end = 0;
			state = State.INIT;
		}

		public boolean nextRecord() throws IOException {
			while (true) {
				switch (state) {
				case INIT:
					boolean eof = !readMore();
					if (eof) {
						state = State.EOF;
						return false;
					} else {
						state = State.IN_RECORD;
						return true;
					}

				case IN_RECORD:
					int p = start;
					while (true) {
						if (p >= end) {
							int s = start;
							eof = !readMore();
							if (eof) {
								state = State.EOF;
								return start < end;
							}
							p -= (s - start);
						}
						char ch = buffer[p];
						if (ch == '\n') {
							start = p + 1;
							state = State.EOR;
							break;
						} else if (ch == '\r') {
							start = p + 1;
							state = State.CR;
							break;
						}
						++p;
					}
					break;

				case CR:
					if (start >= end) {
						eof = !readMore();
						if (eof) {
							state = State.EOF;
							return false;
						}
					}
					char ch = buffer[start];
					if (ch == '\n') {
						++start;
						state = State.EOR;
					} else {
						state = State.IN_RECORD;
						return true;
					}

				case EOR:
					if (start >= end) {
						eof = !readMore();
						if (eof) {
							state = State.EOF;
							return false;
						}
					}
					state = State.IN_RECORD;
					return start < end;

				case EOF:
					return false;
				}
			}
		}

		public boolean nextField() throws IOException {
			switch (state) {
			case INIT:
			case EOR:
			case EOF:
			case CR:
				return false;

			case IN_RECORD:
				boolean eof;
				int p = start;
				while (true) {
					if (p >= end) {
						int s = start;
						eof = !readMore();
						if (eof) {
							state = State.EOF;
							return true;
						}
						p -= (s - start);
					}
					char ch = buffer[p];
					if (ch == fieldDelimiter) {
						fStart = start;
						fEnd = p;
						start = p + 1;
						return true;
					} else if (ch == '\n') {
						fStart = start;
						fEnd = p;
						start = p + 1;
						state = State.EOR;
						return true;
					} else if (ch == '\r') {
						fStart = start;
						fEnd = p;
						start = p + 1;
						state = State.CR;
						return true;
					}
					++p;
				}
			}
			throw new IllegalStateException();
		}

		protected boolean readMore() throws IOException {
			if (start > 0) {
				System.arraycopy(buffer, start, buffer, 0, end - start);
			}
			end -= start;
			start = 0;

			if (end == buffer.length) {
				buffer = Arrays.copyOf(buffer, buffer.length + INCREMENT);
			}

			int n = in.read(buffer, end, buffer.length - end);
			if (n < 0) {
				return false;
			}
			end += n;
			return true;
		}

		public int getfStart() {
			return fStart;
		}

		public void setfStart(int fStart) {
			this.fStart = fStart;
		}

		public int getfEnd() {
			return fEnd;
		}

		public void setfEnd(int fEnd) {
			this.fEnd = fEnd;
		}

		public char[] getBuffer() {
			return buffer;
		}
	}

}
