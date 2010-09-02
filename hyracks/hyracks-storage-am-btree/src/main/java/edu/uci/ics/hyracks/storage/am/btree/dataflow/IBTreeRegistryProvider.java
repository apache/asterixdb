package edu.uci.ics.hyracks.storage.am.btree.dataflow;

import java.io.Serializable;

public interface IBTreeRegistryProvider extends Serializable {
	public BTreeRegistry getBTreeRegistry();
}
