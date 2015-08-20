/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.storage.am.common.tuples;

// encodes positive integers in a variable-byte format

public class VarLenIntEncoderDecoder {
	public static final int ENCODE_MASK = 0x0000007F;
	public static final byte CONTINUE_CHUNK = (byte) 0x80;
	public static final byte DECODE_MASK = (byte) 0x7F;

	private byte[] encTmp = new byte[5];

	private int pos;
	private byte[] bytes;

	public void reset(byte[] bytes, int pos) {
		this.bytes = bytes;
		this.pos = pos;
	}

	public int encode(int val) {
		int origPos = 0;
		int tmpPos = 0;
		while (val > ENCODE_MASK) {
			encTmp[tmpPos++] = (byte) (val & ENCODE_MASK);
			val = val >>> 7;
		}
		encTmp[tmpPos++] = (byte) (val);

		// reverse order to optimize for decoding speed
		for (int i = 0; i < tmpPos - 1; i++) {
			bytes[pos++] = (byte) (encTmp[tmpPos - 1 - i] | CONTINUE_CHUNK);
		}
		bytes[pos++] = encTmp[0];

		return pos - origPos;
	}

	public int decode() {
		int sum = 0;
		while ((bytes[pos] & CONTINUE_CHUNK) == CONTINUE_CHUNK) {
			sum = (sum + (bytes[pos] & DECODE_MASK)) << 7;
			pos++;
		}
		sum += bytes[pos++];
		return sum;
	}

	// calculate the number of bytes needed for encoding
	public int getBytesRequired(int val) {
		int byteCount = 0;
		while (val > ENCODE_MASK) {
			val = val >>> 7;
			byteCount++;
		}
		return byteCount + 1;
	}

	public int getPos() {
		return pos;
	}

	// fast encoding, slow decoding version
	/*
	 * public void encode(int val) { while(val > ENCODE_MASK) { bytes[pos++] =
	 * (byte)(((byte)(val & ENCODE_MASK)) | CONTINUE_CHUNK); val = val >>> 7; }
	 * bytes[pos++] = (byte)(val); }
	 * 
	 * public int decode() { int sum = 0; int shift = 0; while( (bytes[pos] &
	 * CONTINUE_CHUNK) == CONTINUE_CHUNK) { sum = (sum + (bytes[pos] &
	 * DECODE_MASK)) << 7 * shift++; pos++; } sum += bytes[pos++] << 7 * shift;
	 * return sum; }
	 */
}