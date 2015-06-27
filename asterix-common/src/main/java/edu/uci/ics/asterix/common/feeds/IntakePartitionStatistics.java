package edu.uci.ics.asterix.common.feeds;

import java.util.BitSet;

public class IntakePartitionStatistics {

	public static int ACK_WINDOW_SIZE = 1024;
	private int partition;
	private int base;
	private BitSet bitSet;

	public IntakePartitionStatistics(int partition, int base) {
		this.partition = partition;
		this.base = base;
		this.bitSet = new BitSet(ACK_WINDOW_SIZE);
	}

	public void ackRecordId(int recordId) {
		int posIndexWithinBase = recordId % ACK_WINDOW_SIZE;
		this.bitSet.set(posIndexWithinBase);
	}

	public byte[] getAckInfo() {
		return bitSet.toByteArray();
	}

}
