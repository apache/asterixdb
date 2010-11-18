package edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.util.StringUtils;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IBinaryTokenizer;

public class DelimitedUTF8StringBinaryTokenizer implements IBinaryTokenizer {
	
	private static final RecordDescriptor tokenSchema = 
		new RecordDescriptor(new ISerializerDeserializer[] { UTF8StringSerializerDeserializer.INSTANCE } );
	
	private final char delimiter;
	private byte[] data;
	private int start;
	private int length;
		
	private int tokenLength;
	private int tokenStart;
	private int pos;	
		
	public DelimitedUTF8StringBinaryTokenizer(char delimiter) {
		this.delimiter = delimiter;
	}
	
	@Override
	public int getTokenLength() {
		return tokenLength;
	}

	@Override
	public int getTokenStartOff() {
		return tokenStart;
	}

	@Override
	public boolean hasNext() {		
		if(pos >= start + length) return false;
		else return true;						
	}
	
	@Override
	public void next() {
		tokenLength = 0;
		tokenStart = pos;
		while(pos < start + length) {
			int len = StringUtils.charSize(data, pos);			
			char ch = StringUtils.charAt(data, pos);	
			pos += len;
			if(ch == delimiter) {
				break;
			}
			tokenLength += len;
		}
	}
	
	@Override
	public void reset(byte[] data, int start, int length) {		
		this.data = data;
		this.start = start;
		this.pos = start;
		this.length = length;			
		this.tokenLength = 0;
		this.tokenStart = 0; 
        pos += 2; // UTF-8 specific
	}
	
	@Override
	public void writeToken(DataOutput dos) throws IOException {
		// WARNING: 2-byte length indicator is specific to UTF-8
		dos.writeShort((short)tokenLength);
		dos.write(data, tokenStart, tokenLength);
	}

	@Override
	public RecordDescriptor getTokenSchema() {
		return tokenSchema;
	}
}
