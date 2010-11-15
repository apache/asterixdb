package edu.uci.ics.hyracks.storage.am.btree.impls;

import edu.uci.ics.hyracks.storage.am.btree.tuples.VarLenIntEncoderDecoder;

public class Test {
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		byte[] bytes = new byte[100];
			
		VarLenIntEncoderDecoder encDec = new VarLenIntEncoderDecoder();
		
		for(int i = 0; i < 1024; i++) {
			encDec.reset(bytes, 0);
			int bytesWritten = encDec.encode(i);
			encDec.reset(bytes, 0);
			int decoded = encDec.decode();		
			int byteCount = encDec.getBytesRequired(i);			
			System.out.println("ENCODED: " + i + " DECODED: " + decoded + " BYTES: " + bytesWritten + " " + byteCount);
		}		
	}		
}
